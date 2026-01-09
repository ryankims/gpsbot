import os
import json
import pandas as pd
import requests
import math
import io
import time
from datetime import datetime, timedelta, date
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from dateutil import parser

# ================= [설정값 최적화] =================
AUTO_SWITCH_DATE = date(2026, 1, 10) 

# 너무 좁은 반경은 장소 파편화의 원인이 됩니다. 50m로 원복을 권장합니다.
STAY_RADIUS = 50       
MIN_STAY_MINUTES = 5   
MERGE_TIME_GAP_MINUTES = 30  

IS_CSV_UTC = False  
SMOOTHING_WINDOW = 3
ACCURACY_LIMIT = 50

MY_TAG_RULES = {
    "마트": "🛒 Market", "편의점": "🛒 Market", "학교": "🏫 School", "초등": "🏫 School",
    "역": "🚆 Station", "카페": "☕ Cafe", "커피": "☕ Cafe", "다이소": "🛍️ Shopping",
    "집": "🏠 Home", "아파트": "🏠 Home"
}

# ================= [비밀번호 로드] =================
try:
    from secrets import MY_KAKAO_KEY, MY_FOLDER_ID, MY_NOTION_KEY, MY_NOTION_DB_ID
    GDRIVE_SA_KEY = None 
    print("💻 로컬 PC 모드로 실행합니다.")
except ImportError:
    MY_KAKAO_KEY = os.environ.get("KAKAO_API_KEY")
    MY_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")
    MY_NOTION_KEY = os.environ.get("NOTION_KEY")
    MY_NOTION_DB_ID = os.environ.get("NOTION_DB_ID")
    GDRIVE_SA_KEY = os.environ.get("GDRIVE_SA_KEY")
    print("☁️ 서버(Github) 모드로 실행합니다.")

# ================= [기능 함수] =================
def get_credentials():
    if os.path.exists('service_account.json'):
        return service_account.Credentials.from_service_account_file('service_account.json', scopes=['https://www.googleapis.com/auth/drive.readonly'])
    elif GDRIVE_SA_KEY:
        info = json.loads(GDRIVE_SA_KEY)
        return service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive.readonly'])
    return None

def format_duration(minutes):
    minutes = int(minutes)
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}시간 {mins}분" if hours > 0 else f"{mins}분"

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2) * math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

def sync_fix_and_learn():
    print("🔄 노션 데이터 동기화 및 중복 검사 준비 중...")
    url = f"https://api.notion.com/v1/databases/{MY_NOTION_DB_ID}/query"
    headers = {"Authorization": f"Bearer {MY_NOTION_KEY}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    
    payload = {"page_size": 100, "sorts": [{"property": "방문일시", "direction": "descending"}]}
    
    existing_records = [] # (start_time, end_time, place_name)
    name_tag_memory = {} 
    
    try:
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            for page in results:
                props = page.get("properties", {})
                date_prop = props.get("방문일시", {}).get("date", {})
                title_prop = props.get("이름", {}).get("title", [])
                p_name = title_prop[0].get("text", {}).get("content", "") if title_prop else ""

                if date_prop and date_prop.get("start"):
                    s_dt = parser.parse(date_prop["start"]).replace(tzinfo=None)
                    e_dt = parser.parse(date_prop["end"]).replace(tzinfo=None) if date_prop.get("end") else s_dt
                    existing_records.append({'start': s_dt, 'end': e_dt, 'name': p_name})
                
                tag_prop = props.get("태그", {}).get("multi_select", [])
                if p_name and tag_prop:
                    name_tag_memory[p_name] = tag_prop[0]['name']
            print(f"📊 기존 기록 {len(existing_records)}개 로드 완료.")
    except Exception as e:
        print(f"⚠️ 노션 읽기 에러: {e}")
    return existing_records, name_tag_memory

def get_geo_info(lat, lng):
    headers = {"Authorization": f"KakaoAK {MY_KAKAO_KEY}"}
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

def send_to_notion(visit_data, existing_records, name_tag_memory):
    new_start = visit_data['start'].replace(tzinfo=None)
    
    # 중복 체크 강화: 시간대가 5분 이내로 겹치면 중복 처리
    for rec in existing_records:
        if abs((new_start - rec['start']).total_seconds()) < 300:
            print(f"🛡️ [중복 차단] {visit_data['place_name']} ({new_start.strftime('%m/%d %H:%M')})")
            return

    final_tag = "📍 기타"
    if visit_data['place_name'] in name_tag_memory:
        final_tag = name_tag_memory[visit_data['place_name']]
    else:
        for k, t in MY_TAG_RULES.items():
            if k in visit_data['place_name']: final_tag = t; break

    url = "https://api.notion.com/v1/pages"
    headers = {"Authorization": f"Bearer {MY_NOTION_KEY}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    
    payload = {
        "parent": {"database_id": MY_NOTION_DB_ID},
        "properties": {
            "이름": {"title": [{"text": {"content": visit_data['place_name']}}]},
            "주소": {"rich_text": [{"text": {"content": visit_data['address']}}]},
            "태그": {"multi_select": [{"name": final_tag}]},
            "체류시간": {"rich_text": [{"text": {"content": format_duration(visit_data['duration'])}}]},
            "방문일시": {"date": {"start": visit_data['start'].isoformat(), "end": visit_data['end'].isoformat()}},
            "Lat": {"number": visit_data['lat']},
            "Lon": {"number": visit_data['lon']}
        }
    }

    try:
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 200:
            print(f"✅ 등록: {visit_data['place_name']} ({new_start.strftime('%H:%M')})")
            existing_records.append({'start': new_start, 'name': visit_data['place_name']})
        else: print(f"❌ 실패: {resp.text}")
    except Exception as e: print(f"❌ 에러: {e}")

def download_target_files():
    creds = get_credentials()
    if not creds: return []
    service = build('drive', 'v3', credentials=creds)
    
    results = service.files().list(
        q=f"'{MY_FOLDER_ID}' in parents and trashed=false",
        fields="files(id, name, mimeType, createdTime)",
        orderBy='createdTime desc',
        pageSize=1 # 일단 가장 최신 파일 하나만 정확히 처리합시다
    ).execute()
    
    items = results.get('files', [])
    downloaded_files = []
    
    for item in items:
        if not (item['name'].lower().endswith('.csv') or item['mimeType'] == 'application/vnd.google-apps.spreadsheet'):
            continue
        print(f"   ⬇️ 다운로드 중: {item['name']}")
        fh = io.BytesIO()
        try:
            if item['mimeType'] == 'application/vnd.google-apps.spreadsheet':
                request = service.files().export_media(fileId=item['id'], mimeType='text/csv')
            else:
                request = service.files().get_media(fileId=item['id'])
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False: status, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='utf-8-sig')
            downloaded_files.append((df, item['name']))
        except Exception as e:
            print(f"   ❌ {item['name']} 다운로드 실패: {e}")
    return downloaded_files

def process_clustering(df):
    points = df.to_dict('records')
    if not points: return []
    raw_visits = []; anchor = points[0]; cluster = [anchor]
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
                raw_visits.append({'place_name': api_name, 'address': api_addr, 'lat': avg_lat, 'lon': avg_lon, 'start': start_t, 'end': end_t, 'duration': duration})
            anchor = curr; cluster = [curr]
    return raw_visits

def merge_consecutive_visits(visits):
    if not visits: return []
    merged = [visits[0]]
    for current in visits[1:]:
        last = merged[-1]
        # 장소명이 같거나 주소가 유사하면 병합
        is_same_place = (current['place_name'] == last['place_name']) or (current['address'][:10] == last['address'][:10])
        time_gap = (current['start'] - last['end']).total_seconds() / 60
        if is_same_place and time_gap <= MERGE_TIME_GAP_MINUTES:
            last['end'] = current['end']; last['duration'] = (last['end'] - last['start']).total_seconds() / 60
        else: merged.append(current)
    return merged

def main():
    print(f"🚀 GPS 분석기 안정화 버전 (반경:{STAY_RADIUS}m)")
    existing_records, name_tag_memory = sync_fix_and_learn()
    file_list = download_target_files()
    
    for df, filename in file_list:
        df.columns = df.columns.str.strip().str.lower()
        if 'time' not in df.columns and 'date' in df.columns: df['time'] = df['date'] + ' ' + df['time']
        df['datetime'] = pd.to_datetime(df['time'])
        df = df.sort_values('datetime')
        if 'accuracy' in df.columns: df = df[df['accuracy'] <= ACCURACY_LIMIT]
        df['smooth_lat'] = df['lat'].rolling(window=SMOOTHING_WINDOW, min_periods=1, center=True).mean()
        df['smooth_lon'] = df['lon'].rolling(window=SMOOTHING_WINDOW, min_periods=1, center=True).mean()
        
        final_visits = merge_consecutive_visits(process_clustering(df))
        for visit in final_visits:
            send_to_notion(visit, existing_records, name_tag_memory)

if __name__ == "__main__":
    main()
