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
AUTO_SWITCH_DATE = date(2026, 1, 10)

STAY_TIME_MIN = 10            # 최소 체류 시간 (분)
STAY_RADIUS = 30              # 체류 중 허용 이동 반경 (m)
MERGE_TIME_GAP_MINUTES = 30   # 동일 장소 병합 간격

SMOOTHING_WINDOW = 3
ACCURACY_LIMIT = 50

MY_TAG_RULES = {
    "마트": "🛒 Market", "편의점": "🛒 Market",
    "학교": "🏫 School", "초등": "🏫 School",
    "역": "🚆 Station",
    "카페": "☕ Cafe", "커피": "☕ Cafe",
    "다이소": "🛍️ Shopping",
    "집": "🏠 Home", "아파트": "🏠 Home"
}

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

# ================= [유틸] =================
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
    return None

def format_duration(minutes):
    minutes = int(minutes)
    h = minutes // 60
    m = minutes % 60
    return f"{h}시간 {m}분" if h else f"{m}분"

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

# ================= [노션 기존 데이터 로드] =================
def sync_fix_and_learn():
    url = f"https://api.notion.com/v1/databases/{MY_NOTION_DB_ID}/query"
    headers = {
        "Authorization": f"Bearer {MY_NOTION_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    payload = {
        "page_size": 100,
        "sorts": [{"property": "방문일시", "direction": "descending"}]
    }

    existing = []
    name_tag_memory = {}

    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code != 200:
        return existing, name_tag_memory

    for page in resp.json().get("results", []):
        props = page["properties"]
        date_prop = props["방문일시"]["date"]
        title = props["이름"]["title"]
        name = title[0]["text"]["content"] if title else ""

        if date_prop and date_prop["start"]:
            s = parser.parse(date_prop["start"]).replace(tzinfo=None)
            existing.append({"start": s, "name": name})

        tags = props["태그"]["multi_select"]
        if name and tags:
            name_tag_memory[name] = tags[0]["name"]

    return existing, name_tag_memory

# ================= [카카오 API] =================
def get_geo_info(lat, lon):
    headers = {"Authorization": f"KakaoAK {MY_KAKAO_KEY}"}
    addr = "주소 미확인"
    name = ""

    r = requests.get(
        "https://dapi.kakao.com/v2/local/geo/coord2address.json",
        headers=headers,
        params={"x": lon, "y": lat},
        timeout=3
    )
    if r.status_code == 200 and r.json()["meta"]["total_count"] > 0:
        doc = r.json()["documents"][0]
        if doc["road_address"]:
            addr = doc["road_address"]["address_name"]
            name = doc["road_address"]["building_name"] or ""
        else:
            addr = doc["address"]["address_name"]

    if not name:
        name = addr

    return name, addr

PLACE_CACHE = {}

def resolve_place(lat, lon, place_id):
    if place_id in PLACE_CACHE:
        return PLACE_CACHE[place_id]
    name, addr = get_geo_info(lat, lon)
    PLACE_CACHE[place_id] = (name, addr)
    return name, addr

# ================= [체류 감지] =================
def detect_stays(df):
    stays = []
    window = []

    for row in df.itertuples():
        window.append(row)
        duration = (window[-1].datetime - window[0].datetime).total_seconds() / 60

        if duration < STAY_TIME_MIN:
            continue

        lats = [p.smooth_lat for p in window]
        lons = [p.smooth_lon for p in window]

        max_dist = max(
            haversine(lats[0], lons[0], la, lo)
            for la, lo in zip(lats, lons)
        )

        if max_dist <= STAY_RADIUS:
            avg_lat = sum(lats) / len(lats)
            avg_lon = sum(lons) / len(lons)

            stays.append({
                "start": window[0].datetime,
                "end": window[-1].datetime,
                "duration": duration,
                "lat": avg_lat,
                "lon": avg_lon,
                "place_id": f"{round(avg_lat,5)}_{round(avg_lon,5)}"
            })
            window = []
        else:
            window.pop(0)

    return stays

def merge_stays(stays):
    if not stays:
        return []
    merged = [stays[0]]

    for cur in stays[1:]:
        last = merged[-1]
        gap = (cur["start"] - last["end"]).total_seconds() / 60

        if cur["place_id"] == last["place_id"] and gap <= MERGE_TIME_GAP_MINUTES:
            last["end"] = cur["end"]
            last["duration"] = (last["end"] - last["start"]).total_seconds() / 60
        else:
            merged.append(cur)

    return merged

# ================= [노션 전송] =================
def send_to_notion(v, existing, name_tag_memory):
    for rec in existing:
        if abs((v["start"] - rec["start"]).total_seconds()) < 300:
            return

    tag = name_tag_memory.get(v["place_name"], "📍 기타")
    for k, t in MY_TAG_RULES.items():
        if k in v["place_name"]:
            tag = t
            break

    payload = {
        "parent": {"database_id": MY_NOTION_DB_ID},
        "properties": {
            "이름": {"title": [{"text": {"content": v["place_name"]}}]},
            "주소": {"rich_text": [{"text": {"content": v["address"]}}]},
            "태그": {"multi_select": [{"name": tag}]},
            "체류시간": {"rich_text": [{"text": {"content": format_duration(v["duration"])}}]},
            "방문일시": {"date": {"start": v["start"].isoformat(), "end": v["end"].isoformat()}},
            "Lat": {"number": v["lat"]},
            "Lon": {"number": v["lon"]},
            "PlaceID": {"rich_text": [{"text": {"content": v["place_id"]}}]}
        }
    }

    requests.post(
        "https://api.notion.com/v1/pages",
        headers={
            "Authorization": f"Bearer {MY_NOTION_KEY}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        },
        json=payload
    )

# ================= [Drive CSV 로드] =================
def download_target_files():
    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)

    order = "createdTime asc" if datetime.now().date() < AUTO_SWITCH_DATE else "createdTime desc"
    size = 100 if order.endswith("asc") else 1

    res = service.files().list(
        q=f"'{MY_FOLDER_ID}' in parents and trashed=false",
        fields="files(id,name,mimeType,createdTime)",
        orderBy=order,
        pageSize=size
    ).execute()

    files = []
    for f in res.get("files", []):
        if not f["name"].lower().endswith(".csv"):
            continue

        fh = io.BytesIO()
        req = service.files().get_media(fileId=f["id"])
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh, encoding="utf-8-sig")
        files.append((df, f["name"]))

    return files

# ================= [MAIN] =================
def main():
    existing, name_tag_memory = sync_fix_and_learn()
    files = download_target_files()

    for df, name in files:
        df.columns = df.columns.str.lower()
        df["datetime"] = pd.to_datetime(df["time"])
        df = df.sort_values("datetime")

        if "accuracy" in df.columns:
            df = df[df["accuracy"] <= ACCURACY_LIMIT]

        df["smooth_lat"] = df["lat"].rolling(SMOOTHING_WINDOW, center=True, min_periods=1).mean()
        df["smooth_lon"] = df["lon"].rolling(SMOOTHING_WINDOW, center=True, min_periods=1).mean()

        stays = merge_stays(detect_stays(df))

        for s in stays:
            name_, addr_ = resolve_place(s["lat"], s["lon"], s["place_id"])
            send_to_notion({
                **s,
                "place_name": name_,
                "address": addr_
            }, existing, name_tag_memory)

if __name__ == "__main__":
    main()
