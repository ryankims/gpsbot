import os
import json
import math
import io
import pandas as pd
from datetime import datetime
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ================== 사용자 설정 ==================
GDRIVE_FOLDER_ID = "10tC3MvA9gzjv1E3rdBDi6ZyMaf4lSEni"
NOTION_KEY = os.environ.get("NOTION_KEY")
NOTION_DB_ID = "2ddb9d7d1d4a81028e19d09a1386f820"

ACCURACY_LIMIT = 50
MIN_MOVE_DISTANCE = 30  # meters
# ===============================================

def get_credentials():
    if os.path.exists("service_account.json"):
        return service_account.Credentials.from_service_account_file(
            "service_account.json",
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
    info = json.loads(os.environ["GDRIVE_SA_KEY"])
    return service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )


def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def download_all_csv():
    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)

    results = service.files().list(
        q=f"'{GDRIVE_FOLDER_ID}' in parents and trashed=false",
        fields="files(id,name,mimeType)",
        pageSize=100,
    ).execute()

    dfs = []

    for f in results.get("files", []):
        if not f["name"].lower().endswith(".csv"):
            continue

        fh = io.BytesIO()

        if f["mimeType"].startswith("application/vnd.google-apps"):
            request = service.files().export_media(
                fileId=f["id"],
                mimeType="text/csv"
            )
        else:
            request = service.files().get_media(fileId=f["id"])

        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)
        dfs.append(pd.read_csv(fh))

    if not dfs:
        raise RuntimeError("❌ CSV 데이터를 하나도 불러오지 못했습니다.")

    return pd.concat(dfs, ignore_index=True)


# 👇 [수정됨] 중복을 제거하고 안전장치(.strip)를 추가한 유일한 함수
def send_to_notion(summary):
    url = "https://api.notion.com/v1/pages"
    
    # 여기서 NOTION_KEY가 없으면 에러를 띄워서 바로 알 수 있게 함
    if not NOTION_KEY:
        print("❌ 오류: NOTION_KEY 환경변수가 비어있습니다.")
        return

    headers = {
        "Authorization": f"Bearer {NOTION_KEY.strip()}",  # 👈 핵심 수정: 공백 제거!
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    payload = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": {
            "날짜": {"title": [{"text": {"content": summary["date"]}}]},
            "체류 장소": {
                "multi_select": [{"name": p} for p in summary["places"]]
            },
            "이동 요약": {
                "rich_text": [{"text": {"content": summary["route"]}}]
            },
            "총 이동거리(km)": {"number": round(summary["distance_km"], 2)},
            "총 이동시간(분)": {"number": summary["duration_min"]},
            "지도 링크": {"url": summary["map_url"]},
        },
    }

    r = requests.post(url, headers=headers, json=payload)
    if r.status_code == 200:
        print(f"✅ {summary['date']} 등록 완료")
    else:
        print("❌ 노션 오류:", r.text)


def main():
    try:
        df = download_all_csv()
    except Exception as e:
        print(f"⚠️ 데이터 다운로드 실패: {e}")
        return

    df.columns = df.columns.str.lower()
    df["datetime"] = pd.to_datetime(df["time"])

    if "accuracy" in df.columns:
        df = df[df["accuracy"] <= ACCURACY_LIMIT]

    now = datetime.now()
    today = now.date()

    if today <= datetime(2026, 1, 10).date():
        target_df = df[df["datetime"].dt.date <= today]
        print(f"🧱 초기 누적 모드 (~ {today})")
    else:
        target_df = df[df["datetime"].dt.date == today]
        print(f"📆 일일 모드 ({today})")

    target_df["date"] = target_df["datetime"].dt.date

    for date, g in target_df.groupby("date"):
        g = g.sort_values("datetime")
        if len(g) < 2:
            continue

        dist = 0
        path = [g.iloc[0]]

        for i in range(1, len(g)):
            d = haversine(
                g.iloc[i - 1].lat,
                g.iloc[i - 1].lon,
                g.iloc[i].lat,
                g.iloc[i].lon,
            )
            if d >= MIN_MOVE_DISTANCE:
                dist += d
                path.append(g.iloc[i])

        if len(path) < 2:
            continue

        duration = int(
            (g.iloc[-1].datetime - g.iloc[0].datetime).total_seconds() / 60
        )

        coords = "/".join([f"{p.lat},{p.lon}" for p in path])
        # Google Maps URL 수정 (경로 시각화가 더 잘 되도록)
        map_url = f"https://www.google.com/maps/dir/{coords}"

        summary = {
            "date": str(date),
            "places": ["이동"],
            "route": " → ".join(
                [f"{p.lat:.3f},{p.lon:.3f}" for p in path[:5]]
            ),
            "distance_km": dist / 1000,
            "duration_min": duration,
            "map_url": map_url,
        }

        send_to_notion(summary)


if __name__ == "__main__":
    main()
