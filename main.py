import os
import json
import math
import io
import pandas as pd
import requests
from collections import defaultdict
from datetime import datetime
from dateutil import parser

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ==================================================
# 설정
# ==================================================
STAY_TIME_MIN = 10
STAY_RADIUS = 30
SMOOTHING_WINDOW = 3
ACCURACY_LIMIT = 50

# ==================================================
# 환경 변수
# ==================================================
try:
    from secrets import MY_KAKAO_KEY, MY_FOLDER_ID, MY_NOTION_KEY, MY_NOTION_DB_ID
    GDRIVE_SA_KEY = None
    print("💻 로컬 모드")
except ImportError:
    MY_KAKAO_KEY = os.environ["KAKAO_API_KEY"]
    MY_FOLDER_ID = os.environ["GDRIVE_FOLDER_ID"]
    MY_NOTION_KEY = os.environ["NOTION_KEY"]
    MY_NOTION_DB_ID = os.environ["NOTION_DB_ID"]
    GDRIVE_SA_KEY = os.environ["GDRIVE_SA_KEY"]
    print("☁️ 서버 모드")

# ==================================================
# Google Drive 인증
# ==================================================
def get_credentials():
    if os.path.exists("service_account.json"):
        return service_account.Credentials.from_service_account_file(
            "service_account.json",
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
    return service_account.Credentials.from_service_account_info(
        json.loads(GDRIVE_SA_KEY),
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )

# ==================================================
# 유틸
# ==================================================
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def format_duration(mins):
    mins = int(mins)
    h = mins // 60
    m = mins % 60
    return f"{h}시간 {m}분" if h else f"{m}분"

# ==================================================
# Notion 기존 방문 시작시각 로드
# ==================================================
def load_existing_starts():
    url = f"https://api.notion.com/v1/databases/{MY_NOTION_DB_ID}/query"
    headers = {
        "Authorization": f"Bearer {MY_NOTION_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }

    resp = requests.post(url, headers=headers).json()
    starts = []

    for r in resp.get("results", []):
        d = r["properties"]["방문일시"]["date"]
        if d and d.get("start"):
            dt = parser.parse(d["start"])
            if dt.tzinfo:
                dt = dt.astimezone(None).replace(tzinfo=None)
            starts.append(dt)

    return starts

# ==================================================
# Kakao API
# ==================================================
PLACE_CACHE = {}

def get_place(lat, lon):
    key = (round(lat, 5), round(lon, 5))
    if key in PLACE_CACHE:
        return PLACE_CACHE[key]

    headers = {"Authorization": f"KakaoAK {MY_KAKAO_KEY}"}
    resp = requests.get(
        "https://dapi.kakao.com/v2/local/geo/coord2address.json",
        headers=headers,
        params={"x": lon, "y": lat},
        timeout=5
    ).json()

    doc = resp["documents"][0]
    addr = (
        doc["road_address"]["address_name"]
        if doc["road_address"]
        else doc["address"]["address_name"]
    )

    name = (
        doc["road_address"]["building_name"]
        if doc["road_address"] and doc["road_address"]["building_name"]
        else addr
    )

    PLACE_CACHE[key] = (name, addr)
    return name, addr

# ==================================================
# 체류 감지
# ==================================================
def detect_stays(df):
    stays = []
    buf = []

    for r in df.itertuples():
        buf.append(r)

        duration = (buf[-1].datetime - buf[0].datetime).total_seconds() / 60
        if duration < STAY_TIME_MIN:
            continue

        max_dist = max(
            haversine(
                buf[0].smooth_lat,
                buf[0].smooth_lon,
                p.smooth_lat,
                p.smooth_lon
            )
            for p in buf
        )

        if max_dist <= STAY_RADIUS:
            stays.append({
                "start": buf[0].datetime,
                "end": buf[-1].datetime,
                "duration": duration,
                "lat": sum(p.smooth_lat for p in buf) / len(buf),
                "lon": sum(p.smooth_lon for p in buf) / len(buf)
            })
            buf = []
        else:
            buf.pop(0)

    return stays

# ==================================================
# 같은 장소 + 같은 날짜 병합
# ==================================================
def merge_daily(stays):
    groups = defaultdict(list)

    for s in stays:
        key = (
            round(s["lat"], 5),
            round(s["lon"], 5),
            s["start"].date()
        )
        groups[key].append(s)

    merged = []
    for items in groups.values():
        items.sort(key=lambda x: x["start"])
        merged.append({
            "start": items[0]["start"],
            "end": items[-1]["end"],
            "duration": sum(i["duration"] for i in items),
            "lat": items[0]["lat"],
            "lon": items[0]["lon"]
        })

    return merged

# ==================================================
# Google Drive 다운로드
# ==================================================
def download_files():
    service = build("drive", "v3", credentials=get_credentials())

    res = service.files().list(
        q=f"'{MY_FOLDER_ID}' in parents and trashed=false",
        fields="files(id,name,mimeType)",
        orderBy="createdTime desc",
        pageSize=5
    ).execute()

    dfs = []

    for f in res.get("files", []):
        fh = io.BytesIO()

        if f["mimeType"] == "application/vnd.google-apps.spreadsheet":
            req = service.files().export_media(
                fileId=f["id"],
                mimeType="text/csv"
            )
        else:
            req = service.files().get_media(fileId=f["id"])

        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)
        dfs.append(pd.read_csv(fh, encoding="utf-8-sig"))

    return dfs

# ==================================================
# Notion 전송
# ==================================================
def send_to_notion(v):
    payload = {
        "parent": {"database_id": MY_NOTION_DB_ID},
        "properties": {
            "이름": {"title": [{"text": {"content": v["name"]}}]},
            "주소": {"rich_text": [{"text": {"content": v["addr"]}}]},
            "체류시간": {
                "rich_text": [{"text": {"content": format_duration(v["duration"])}}]
            },
            "방문일시": {
                "date": {
                    "start": v["start"].isoformat(),
                    "end": v["end"].isoformat()
                }
            },
            "Lat": {"number": v["lat"]},
            "Lon": {"number": v["lon"]}
        }
    }

    requests.post(
        "https://api.notion.com/v1/pages",
        headers={
            "Authorization": f"Bearer {MY_NOTION_KEY}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        },
        json=payload
    )

# ==================================================
# MAIN
# ==================================================
def main():
    existing_starts = load_existing_starts()
    dfs = download_files()

    for df in dfs:
        df.columns = df.columns.str.lower()
        df["datetime"] = pd.to_datetime(df["time"]).dt.tz_localize(None)
        df = df.sort_values("datetime")

        if "accuracy" in df.columns:
            df = df[df["accuracy"] <= ACCURACY_LIMIT]

        df["smooth_lat"] = df["lat"].rolling(
            SMOOTHING_WINDOW, center=True, min_periods=1
        ).mean()
        df["smooth_lon"] = df["lon"].rolling(
            SMOOTHING_WINDOW, center=True, min_periods=1
        ).mean()

        stays = merge_daily(detect_stays(df))

        for s in stays:
            if any(abs((s["start"] - e).total_seconds()) < 300 for e in existing_starts):
                continue

            name, addr = get_place(s["lat"], s["lon"])
            send_to_notion({**s, "name": name, "addr": addr})

# ==================================================
if __name__ == "__main__":
    main()
