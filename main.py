import os
import json
import pandas as pd
import requests
import math
import io
from datetime import datetime, date
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from dateutil import parser

# ================= [설정값] =================
STAY_TIME_MIN = 10
STAY_RADIUS = 30
MERGE_TIME_GAP_MINUTES = 30

SMOOTHING_WINDOW = 3
ACCURACY_LIMIT = 50

# ================= [비밀키 로드] =================
try:
    from secrets import MY_KAKAO_KEY, MY_FOLDER_ID, MY_NOTION_KEY, MY_NOTION_DB_ID
    GDRIVE_SA_KEY = None
    print("💻 로컬 모드")
except ImportError:
    MY_KAKAO_KEY = os.environ.get("KAKAO_API_KEY")
    MY_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")
    MY_NOTION_KEY = os.environ.get("NOTION_KEY")
    MY_NOTION_DB_ID = os.environ.get("NOTION_DB_ID")
    GDRIVE_SA_KEY = os.environ.get("GDRIVE_SA_KEY")
    print("☁️ 서버 모드")

# ================= [Google 인증] =================
def get_credentials():
    if os.path.exists("service_account.json"):
        return service_account.Credentials.from_service_account_file(
            "service_account.json",
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
    if GDRIVE_SA_KEY:
        return service_account.Credentials.from_service_account_info(
            json.loads(GDRIVE_SA_KEY),
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
    raise RuntimeError("Google Drive 인증 실패")

# ================= [유틸] =================
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

def format_duration(minutes):
    m = int(minutes)
    h = m // 60
    m %= 60
    return f"{h}시간 {m}분" if h else f"{m}분"

# ================= [노션 기존 데이터] =================
def load_existing_notion():
    url = f"https://api.notion.com/v1/databases/{MY_NOTION_DB_ID}/query"
    headers = {
        "Authorization": f"Bearer {MY_NOTION_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }

    res = requests.post(url, headers=headers).json()
    existing = []

    for r in res.get("results", []):
        d = r["properties"]["방문일시"]["date"]
        if d and d["start"]:
            dt = parser.parse(d["start"])
            if dt.tzinfo:
                dt = dt.astimezone(None).replace(tzinfo=None)
            existing.append(dt)

    return existing

# ================= [카카오 API] =================
def get_place(lat, lon):
    headers = {"Authorization": f"KakaoAK {MY_KAKAO_KEY}"}
    r = requests.get(
        "https://dapi.kakao.com/v2/local/geo/coord2address.json",
        headers=headers,
        params={"x": lon, "y": lat}
    ).json()

    doc = r["documents"][0]
    addr = doc["road_address"]["address_name"] if doc["road_address"] else doc["address"]["address_name"]
    name = doc["road_address"]["building_name"] if doc["road_address"] else addr
    return name or addr, addr

# ================= [체류 감지] =================
def detect_stays(df):
    stays = []
    window = []

    for r in df.itertuples():
        window.append(r)
        duration = (window[-1].datetime - window[0].datetime).total_seconds() / 60

        if duration < STAY_TIME_MIN:
            continue

        dist = max(
            haversine(window[0].smooth_lat, window[0].smooth_lon, p.smooth_lat, p.smooth_lon)
            for p in window
        )

        if dist <= STAY_RADIUS:
            stays.append({
                "start": window[0].datetime,
                "end": window[-1].datetime,
                "duration": duration,
                "lat": sum(p.smooth_lat for p in window) / len(window),
                "lon": sum(p.smooth_lon for p in window) / len(window)
            })
            window = []
        else:
            window.pop(0)

    return stays

# ================= [Drive 파일 로드] =================
def download_target_files():
    service = build("drive", "v3", credentials=get_credentials())

    res = service.files().list(
        q=f"'{MY_FOLDER_ID}' in parents and trashed=false",
        fields="files(id,name,mimeType,createdTime)",
        orderBy="createdTime desc",
        pageSize=5
    ).execute()

    results = []

    for f in res["files"]:
        fh = io.BytesIO()

        if f["mimeType"] == "text/csv":
            req = service.files().get_media(fileId=f["id"])
        elif f["mimeType"] == "application/vnd.google-apps.spreadsheet":
            req = service.files().export_media(fileId=f["id"], mimeType="text/csv")
        else:
            continue

        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)
        results.append(pd.read_csv(fh, encoding="utf-8-sig"))

    return results

# ================= [노션 전송] =================
def send_to_notion(v):
    payload = {
        "parent": {"database_id": MY_NOTION_DB_ID},
        "properties": {
            "이름": {"title": [{"text": {"content": v["name"]}}]},
            "주소": {"rich_text": [{"text": {"content": v["addr"]}}]},
            "체류시간": {"rich_text": [{"text": {"content": format_duration(v["duration"])}}]},
            "방문일시": {"date": {"start": v["start"].isoformat(), "end": v["end"].isoformat()}},
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

# ================= [MAIN] =================
def main():
    existing = load_existing_notion()
    dfs = download_target_files()

    for df in dfs:
        df.columns = df.columns.str.lower()
        df["datetime"] = pd.to_datetime(df["time"]).dt.tz_localize(None)
        df = df.sort_values("datetime")

        if "accuracy" in df.columns:
            df = df[df["accuracy"] <= ACCURACY_LIMIT]

        df["smooth_lat"] = df["lat"].rolling(SMOOTHING_WINDOW, center=True, min_periods=1).mean()
        df["smooth_lon"] = df["lon"].rolling(SMOOTHING_WINDOW, center=True, min_periods=1).mean()

        stays = detect_stays(df)

        for s in stays:
            if any(abs((s["start"] - e).total_seconds()) < 300 for e in existing):
                continue

            name, addr = get_place(s["lat"], s["lon"])
            send_to_notion({**s, "name": name, "addr": addr})

if __name__ == "__main__":
    main()
