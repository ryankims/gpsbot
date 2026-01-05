import os
import json
import pandas as pd
import requests
import math
import io
import time
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ================= [비밀번호 로드: 하이브리드 방식] =================
# 1. 먼저 내 컴퓨터(secrets.py)에 있는지 확인해봅니다.
try:
    from secrets import MY_KAKAO_KEY, MY_FOLDER_ID, MY_NOTION_KEY, MY_NOTION_DB_ID
    GDRIVE_SA_KEY = None # 로컬에서는 파일로 처리하므로 변수는 비워둠
    print("💻 내 컴퓨터 모드로 실행합니다. (secrets.py 사용)")

# 2. 없으면(Github 서버라면) 환경변수(Secrets)에서 가져옵니다.
except ImportError:
    MY_KAKAO_KEY = os.environ.get("KAKAO_API_KEY")
    MY_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")
    MY_NOTION_KEY = os.environ.get("NOTION_KEY")
    MY_NOTION_DB_ID = os.environ.get("NOTION_DB_ID")
    GDRIVE_SA_KEY = os.environ.get("GDRIVE_SA_KEY")
    print("☁️ Github 서버 모드로 실행합니다. (Secrets 사용)")

# ===================================================================

# [기본 규칙]
MY_TAG_RULES = {
    "마트": "🛒 Market", "편의점": "🛒 Market", "학교": "🏫 School", "초등": "🏫 School",
    "역": "🚆 Station", "카페": "☕ Cafe", "커피": "☕ Cafe", "다이소": "🛍️ Shopping",
    "집": "🏠 Home", "아파트": "🏠 Home"
}

# 설정값
SMOOTHING_WINDOW = 3
ACCURACY_LIMIT = 50
STAY_RADIUS = 100
MIN_STAY_MINUTES = 5

def get_credentials():
    # 1. (내 컴퓨터) service_account.json 파일이 있으면 사용
    if os.path.exists('service_account.json'):
        return service_account.Credentials.from_service_account_file('service_account.json', scopes=['https://www.googleapis.com/auth/drive.readonly'])
    
    # 2. (Github 서버) 환경변수에 내용이 들어있으면 사용
    elif GDRIVE_SA_KEY:
        info = json.loads(GDRIVE_SA_KEY)
        return service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive.readonly'])
    return None

def get_kakao_key(): return MY_KAKAO_KEY
def get_folder_id(): return MY_FOLDER_ID
def get_notion_key(): return MY_NOTION_KEY
def get_notion_db_id(): return MY_NOTION_DB_ID

def format_duration(minutes):
    minutes = int(minutes)
    hours = minutes // 60
    mins = minutes % 60
    if hours > 0: return f"{hours}시간 {mins}분"
    else: return f"{mins}분"

def get_time_fingerprint(iso_string):
    if not iso_string: return ""
    return iso_string[:16]

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2) * math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

def search_place_by_name(keyword, lat, lng):
    api_key = get_kakao_key()
    headers = {"Authorization": f"KakaoAK {api_key}"}
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"
    try:
        params = {"query": keyword}
        if lat and lng:
            params.update({"x": lng, "y": lat, "radius": 1000, "sort": "distance"})
        resp = requests.get(url, headers=headers, params=params, timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            if data['meta']['total_count'] > 0:
                doc = data['documents'][0]
                return doc['place_name'], (doc['road_address_name'] or doc['address_name'])
    except: pass
    return keyword, None

def update_notion_address(page_id, new_address):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    headers = {"Authorization": f"Bearer {get_notion_key()}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    payload = {"properties": {"주소": {"rich_text": [{"text": {"content": new_address}}]}}}
    requests.patch(url, headers=headers, json=payload)
    print(f"   ✨ 주소 자동 보정 완료: {new_address}")

def sync_fix_and_learn():
    print("🔄 노션 데이터 동기화 중...")
    url = f"https://api.notion.com/v1/databases/{get_notion_db_id()}/query"
    headers = {"Authorization": f"Bearer {get_notion_key()}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    payload = {"page_size": 50, "sorts": [{"property": "방문일시", "direction": "descending"}]}
    
    existing_timestamps = set()
    name_tag_memory = {} 
    
    try:
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            for page in results:
                props = page.get("properties", {})
                page_id = page.get("id")
                
                date_prop = props.get("방문일시", {}).get("date", {})
                start_time = date_prop.get("start", "") if date_prop else ""
                
                title_prop = props.get("이름", {}).get("title", [])
                place_name = title_prop[0].get("text", {}).get("content", "") if title_prop else ""

                addr_prop = props.get("주소", {}).get("rich_text", [])
                current_addr = addr_prop[0].get("text", {}).get("content", "") if addr_prop else ""
                
                tag_prop = props.get("태그", {}).get("multi_select", [])
                tag_name = tag_prop[0]['name'] if tag_prop else ""
                
                lat = props.get("Lat", {}).get("number")
                lon = props.get("Lon", {}).get("number")

                if start_time:
                    existing_timestamps.add(get_time_fingerprint(start_time))
                
                if place_name and tag_name:
                    name_tag_memory[place_name] = tag_name

                if place_name and current_addr and lat and lon:
                    if len(place_name) > 2 and place_name not in ["Home", "School", "Work", "Mart"]:
                        if place_name[:2] not in current_addr:
                            real_name, real_addr = search_place_by_name(place_name, lat, lon)
                            if real_addr and (real_addr.replace(" ", "") != current_addr.replace(" ", "")):
                                print(f"🔧 불일치 감지! 이름('{place_name}')에 맞춰 주소 수정.")
                                update_notion_address(page_id, real_addr)

    except Exception as e:
        print(f"⚠️ 노션 동기화 에러: {e}")
        
    return existing_timestamps, name_tag_memory

def get_geo_info(lat, lng):
    api_key = get_kakao_key()
    headers = {"Authorization": f"KakaoAK {api_key}"}
    url_addr = "https://dapi.kakao.com/v2/local/geo/coord2address.json"
    address_str = "주소 미확인"; place_name = ""
    try:
        resp = requests.get(url_addr, headers=headers, params={"x": lng, "y": lat}, timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            if data['meta']['total_count'] > 0:
                doc = data['documents'][0]
                if doc['road_address']:
                    address_str = doc['road_address']['address_name']
                    if doc['road_address']['building_name']: place_name = doc['road_address']['building_name']
                else: address_str = doc['address']['address_name']
    except: pass

    if address_str != "주소 미확인":
        url_kwd = "https://dapi.kakao.com/v2/local/search/keyword.json"
        try:
            params = {"query": address_str, "x": lng, "y": lat, "radius": 50, "sort": "distance"}
            resp = requests.get(url_kwd, headers=headers, params=params, timeout=3)
            if resp.status_code == 200:
                data = resp.json()
                if data['meta']['total_count'] > 0: place_name = data['documents'][0]['place_name']
        except: pass
    if not place_name: place_name = address_str
    return place_name, address_str

def send_to_notion(visit_data, existing_timestamps, name_tag_memory):
    final_tag = "📍 기타"
    if visit_data['place_name'] in name_tag_memory:
        final_tag = name_tag_memory[visit_data['place_name']]
    else:
        for k, t in MY_TAG_RULES.items():
            if k in visit_data['place_name']: final_tag = t; break

    start_iso = visit_data['start'].isoformat()
    time_key = get_time_fingerprint(start_iso)
    
    if time_key in existing_timestamps:
        print(f"⏭️  [중복] 패스: {visit_data['place_name']}")
        return

    url = "https://api.notion.com/v1/pages"
    headers = {"Authorization": f"Bearer {get_notion_key()}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    
    payload = {
        "parent": {"database_id": get_notion_db_id()},
        "properties": {
            "이름": {"title": [{"text": {"content": visit_data['place_name']}}]},
            "주소": {"rich_text": [{"text": {"content": visit_data['address']}}]},
            "태그": {"multi_select": [{"name": final_tag}]},
            "체류시간": {"rich_text": [{"text": {"content": format_duration(visit_data['duration'])}}]},
            "방문일시": {"date": {"start": start_iso, "end": visit_data['end'].isoformat()}},
            "Lat": {"number": visit_data['lat']},
            "Lon": {"number": visit_data['lon']}
        }
    }

    try:
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 200:
            print(f"✅ 등록: {visit_data['place_name']} [{final_tag}]")
            existing_timestamps.add(time_key)
        else: print(f"❌ 실패: {resp.text}")
    except Exception as e: print(f"❌ 에러: {e}")

def download_latest_file():
    creds = get_credentials()
    if not creds: return None
    service = build('drive', 'v3', credentials=creds)
    folder_id = get_folder_id()
    print(f"🔎 최신 로그 검색 중...")
    results = service.files().list(q=f"'{folder_id}' in parents and trashed=false", orderBy='createdTime desc', pageSize=50, fields="files(id, name, mimeType)").execute()
    items = results.get('files', [])
    target_file = None
    for item in items:
        if item['name'].lower().endswith('.csv'): target_file = item; break
    if not target_file: print("❌ CSV 파일이 없습니다."); return None
    print(f"📂 파일 분석: {target_file['name']}")
    fh = io.BytesIO()
    if 'application/vnd.google-apps' in target_file['mimeType']: request = service.files().export_media(fileId=target_file['id'], mimeType='text/csv')
    else: request = service.files().get_media(fileId=target_file['id'])
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False: status, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh), target_file['name']

def main():
    print("🚀 [GPS 분석기] 하이브리드 모드 가동...")
    
    existing_timestamps, name_tag_memory = sync_fix_and_learn()
    print(f"🧠 학습된 태그 규칙: {len(name_tag_memory)}개")

    data = download_latest_file()
    if not data: return
    df, filename = data
    df.columns = df.columns.str.strip().str.lower()
    
    if 'time' not in df.columns and 'date' in df.columns: df['time'] = df['date'] + ' ' + df['time']
    df['datetime'] = pd.to_datetime(df['time'])
    df = df.sort_values('datetime')
    if 'accuracy' in df.columns: df = df[df['accuracy'] <= ACCURACY_LIMIT]
    if len(df) == 0: print("❌ 유효한 데이터가 없습니다."); return

    df['smooth_lat'] = df['lat'].rolling(window=SMOOTHING_WINDOW, min_periods=1, center=True).mean()
    df['smooth_lon'] = df['lon'].rolling(window=SMOOTHING_WINDOW, min_periods=1, center=True).mean()

    points = df.to_dict('records')
    if not points: return
    anchor = points[0]; cluster = [anchor]
    
    for i in range(1, len(points)):
        curr = points[i]
        dist = haversine(anchor['smooth_lat'], anchor['smooth_lon'], curr['smooth_lat'], curr['smooth_lon'])
        
        if dist < STAY_RADIUS: cluster.append(curr)
        else:
            start_t = cluster[0]['datetime']; end_t = cluster[-1]['datetime']
            duration = (end_t - start_t).total_seconds() / 60
            if duration >= MIN_STAY_MINUTES:
                avg_lat = sum(p['smooth_lat'] for p in cluster) / len(cluster)
                avg_lon = sum(p['smooth_lon'] for p in cluster) / len(cluster)
                api_name, api_addr = get_geo_info(avg_lat, avg_lon)
                visit_info = {'place_name': api_name, 'address': api_addr, 'lat': avg_lat, 'lon': avg_lon, 'duration': duration, 'start': start_t, 'end': end_t}
                send_to_notion(visit_info, existing_timestamps, name_tag_memory)
            anchor = curr; cluster = [curr]
            
    if cluster:
        start_t = cluster[0]['datetime']; end_t = cluster[-1]['datetime']
        duration = (end_t - start_t).total_seconds() / 60
        if duration >= MIN_STAY_MINUTES:
            avg_lat = sum(p['smooth_lat'] for p in cluster)/len(cluster)
            avg_lon = sum(p['smooth_lon'] for p in cluster)/len(cluster)
            api_name, api_addr = get_geo_info(avg_lat, avg_lon)
            visit_info = {'place_name': api_name, 'address': api_addr, 'lat': avg_lat, 'lon': avg_lon, 'duration': duration, 'start': start_t, 'end': end_t}
            send_to_notion(visit_info, existing_timestamps, name_tag_memory)

    print(f"\n🎉 완료!")

if __name__ == "__main__":
    main()
