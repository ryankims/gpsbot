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

# ================= [비밀번호 로드] =================
try:
    from secrets import MY_KAKAO_KEY, MY_FOLDER_ID, MY_NOTION_KEY, MY_NOTION_DB_ID
    GDRIVE_SA_KEY = None 
    print("💻 내 컴퓨터 모드로 실행합니다.")
except ImportError:
    MY_KAKAO_KEY = os.environ.get("KAKAO_API_KEY")
    MY_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")
    MY_NOTION_KEY = os.environ.get("NOTION_KEY")
    MY_NOTION_DB_ID = os.environ.get("NOTION_DB_ID")
    GDRIVE_SA_KEY = os.environ.get("GDRIVE_SA_KEY")
    print("☁️ Github 서버 모드로 실행합니다.")

# ================= [설정값] =================
# [자동 모드 전환 기준일]
AUTO_SWITCH_DATE = date(2026, 1, 10) 

IS_CSV_UTC = False  
SMOOTHING_WINDOW = 3
ACCURACY_LIMIT = 50
STAY_RADIUS = 100
MIN_STAY_MINUTES = 5
MERGE_TIME_GAP_MINUTES = 30

MY_TAG_RULES = {
    "마트": "🛒 Market", "편의점": "🛒 Market", "학교": "🏫 School", "초등": "🏫 School",
    "역": "🚆 Station", "카페": "☕ Cafe", "커피": "☕ Cafe", "다이소": "🛍️ Shopping",
    "집": "🏠 Home", "아파트": "🏠 Home"
}

# ================= [기능 함수] =================
def get_credentials():
    if os.path.exists('service_account.json'):
        return service_account.Credentials.from_service_account_file('service_account.json', scopes=['https://www.googleapis.com/auth/drive.readonly'])
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
    return f"{hours}시간 {mins}분" if hours > 0 else f"{mins}분"

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

def sync_fix_and_learn():
    print("🔄 노션 데이터 읽어오는 중...")
    url = f"https://api.notion.com/v1/databases/{get_notion_db_id()}/query"
    headers = {"Authorization": f"Bearer {get_notion_key()}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    
    # 과거 데이터를 많이 넣을 수 있으므로 최근 200개까지 확인
    payload = {"page_size": 200, "sorts": [{"property": "방문일시", "direction": "descending"}]}
    
    existing_ranges = [] 
    name_tag_memory = {} 
    
    try:
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            print(f"📊 노션 기존 기록 {len(results)}개 로드 완료.")
            
            for page in results:
                props = page.get("properties", {})
                date_prop = props.get("방문일시", {}).get("date", {})
                if date_prop:
                    start_str = date_prop.get("start")
                    end_str = date_prop.get("end")
                    if start_str and end_str:
                        try:
                            s_dt = parser.parse(start_str).replace(tzinfo=None)
                            e_dt = parser.parse(end_str).replace(tzinfo=None)
                            title_prop = props.get("이름", {}).get("title", [])
                            p_name = title_prop[0].get("text", {}).get("content", "") if title_prop else "Unknown"
                            existing_ranges.append((s_dt, e_dt, p_name))
                        except: pass
                
                title_prop = props.get("이름", {}).get("title", [])
                p_name = title_prop[0].get("text", {}).get("content", "") if title_prop else ""
                tag_prop = props.get("태그", {}).get("multi_select", [])
                if p_name and tag_prop:
                    name_tag_memory[p_name] = tag_prop[0]['name']

    except Exception as e:
        print(f"⚠️ 노션 읽기 에러: {e}")
        
    return existing_ranges, name_tag_memory

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

def is_overlapping(new_start, new_end, existing_ranges):
    ns = new_start.replace(tzinfo=None)
    ne = new_end.replace(tzinfo=None)
    for (ex_start, ex_end, ex_name) in existing_ranges:
        time_diff = abs((ns - ex_start).total_seconds())
        if time_diff < 120: 
            return True, ex_name
    return False, None

def send_to_notion(visit_data, existing_ranges, name_tag_memory):
    is_dup, dup_name = is_overlapping(visit_data['start'], visit_data['end'], existing_ranges)
    
    if is_dup:
        print(f"🛡️ [중복 차단] 패스: {dup_name} ({visit_data['start'].strftime('%m/%d %H:%M')})")
        return

    final_tag = "📍 기타"
    if visit_data['place_name'] in name_tag_memory:
        final_tag = name_tag_memory[visit_data['place_name']]
    else:
        for k, t in MY_TAG_RULES.items():
            if k in visit_data['place_name']: final_tag = t; break

    url = "https://api.notion.com/v1/pages"
    headers = {"Authorization": f"Bearer {get_notion_key()}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    
    payload = {
        "parent": {"database_id": get_notion_db_id()},
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
            print(f"✅ 등록: {visit_data['place_name']} ({visit_data['start'].strftime('%m/%d %H:%M')})")
            existing_ranges.append((visit_data['start'].replace(tzinfo=None), visit_data['end'].replace(tzinfo=None), visit_data['place_name']))
        else: print(f"❌ 실패: {resp.text}")
    except Exception as e: print(f"❌ 에러: {e}")

# [핵심] 날짜에 따라 파일 가져오는 방식을 바꿈
def download_target_files():
    creds = get_credentials()
    if not creds: return []
    service = build('drive', 'v3', credentials=creds)
    folder_id = get_folder_id()
    
    today = datetime.now().date()
    
    # 1. 날짜 확인 및 모드 결정
    if today < AUTO_SWITCH_DATE:
        print(f"🗓️ 오늘은 {today}입니다. (기준일 {AUTO_SWITCH_DATE} 이전)")
        print("📂 [전체 모드] 과거 데이터를 포함해 모든 CSV 파일을 가져옵니다.")
        # 과거 파일부터 순서대로 처리하기 위해 createdTime asc(오름차순) 사용
        query_params = {'orderBy': 'createdTime asc', 'pageSize': 100} 
    else:
        print(f"🗓️ 오늘은 {today}입니다. (기준일 {AUTO_SWITCH_DATE} 이후)")
        print("📂 [최신 모드] 가장 최근 파일 1개만 가져옵니다.")
        # 최신 파일만 처리하기 위해 createdTime desc(내림차순) 사용
        query_params = {'orderBy': 'createdTime desc', 'pageSize': 1}

    results = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id, name, mimeType, createdTime)",
        **query_params
    ).execute()
    
    items = results.get('files', [])
    if not items: 
        print("❌ CSV 파일이 없습니다.")
        return []

    downloaded_files = []
    print(f"🔎 총 {len(items)}개의 파일을 처리합니다.")

    for item in items:
        if not item['name'].lower().endswith('.csv'): continue
        
        print(f"   ⬇️ 다운로드 중: {item['name']}")
        fh = io.BytesIO()
        if 'application/vnd.google-apps' in item['mimeType']:
            request = service.files().export_media(fileId=item['id'], mimeType='text/csv')
        else:
            request = service.files().get_media(fileId=item['id'])
            
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False: status, done = downloader.next_chunk()
        fh.seek(0)
        downloaded_files.append((pd.read_csv(fh), item['name']))
        
    return downloaded_files

def process_clustering(df):
    points = df.to_dict('records')
    if not points: return []
    
    raw_visits = []
    anchor = points[0]
    cluster = [anchor]

    for i in range(1, len(points)):
        curr = points[i]
        dist = haversine(anchor['smooth_lat'], anchor['smooth_lon'], curr['smooth_lat'], curr['smooth_lon'])

        if dist < STAY_RADIUS:
            cluster.append(curr)
        else:
            start_t = cluster[0]['datetime']; end_t = cluster[-1]['datetime']
            duration = (end_t - start_t).total_seconds() / 60

            if duration >= MIN_STAY_MINUTES:
                avg_lat = sum(p['smooth_lat'] for p in cluster) / len(cluster)
                avg_lon = sum(p['smooth_lon'] for p in cluster) / len(cluster)
                api_name, api_addr = get_geo_info(avg_lat, avg_lon)
                
                raw_visits.append({
                    'place_name': api_name, 'address': api_addr, 'lat': avg_lat, 'lon': avg_lon,
                    'start': start_t, 'end': end_t, 'duration': duration
                })
            anchor = curr; cluster = [curr]

    if cluster:
        start_t = cluster[0]['datetime']; end_t = cluster[-1]['datetime']
        duration = (end_t - start_t).total_seconds() / 60
        if duration >= MIN_STAY_MINUTES:
            avg_lat = sum(p['smooth_lat'] for p in cluster) / len(cluster)
            avg_lon = sum(p['smooth_lon'] for p in cluster) / len(cluster)
            api_name, api_addr = get_geo_info(avg_lat, avg_lon)
            raw_visits.append({
                'place_name': api_name, 'address': api_addr, 'lat': avg_lat, 'lon': avg_lon,
                'start': start_t, 'end': end_t, 'duration': duration
            })
            
    return raw_visits

def merge_consecutive_visits(visits):
    if not visits: return []
    merged = [visits[0]]
    
    for current in visits[1:]:
        last = merged[-1]
        is_same_place = (current['place_name'] == last['place_name']) or \
                        (current['address'].replace(" ", "") == last['address'].replace(" ", ""))
        time_gap = (current['start'] - last['end']).total_seconds() / 60
        
        if is_same_place and time_gap <= MERGE_TIME_GAP_MINUTES:
            last['end'] = current['end'] 
            last['duration'] = (last['end'] - last['start']).total_seconds() / 60
        else:
            merged.append(current)
            
    return merged

def main():
    print("🚀 [GPS 분석기] v2.1 (스마트 날짜 모드)")
    
    existing_ranges, name_tag_memory = sync_fix_and_learn()
    
    # 여기서 날짜에 따라 파일 1개 또는 여러 개를 받아옵니다
    file_list = download_target_files()
    
    if not file_list: return

    # 파일이 여러 개일 수 있으므로 반복문으로 처리
    for df, filename in file_list:
        print(f"\n📄 [파일 처리 시작] {filename}")
        
        df.columns = df.columns.str.strip().str.lower()
        if 'time' not in df.columns and 'date' in df.columns: 
            df['time'] = df['date'] + ' ' + df['time']
        df['datetime'] = pd.to_datetime(df['time'])

        if IS_CSV_UTC:
            df['datetime'] = df['datetime'] + timedelta(hours=9)
        
        df = df.sort_values('datetime')
        if 'accuracy' in df.columns: df = df[df['accuracy'] <= ACCURACY_LIMIT]
        
        if len(df) == 0: 
            print("   ⚠️ 데이터가 없거나 유효하지 않습니다.")
            continue

        df['smooth_lat'] = df['lat'].rolling(window=SMOOTHING_WINDOW, min_periods=1, center=True).mean()
        df['smooth_lon'] = df['lon'].rolling(window=SMOOTHING_WINDOW, min_periods=1, center=True).mean()

        raw_visits = process_clustering(df)
        final_visits = merge_consecutive_visits(raw_visits)

        print(f"   👉 방문 기록 {len(final_visits)}건 발견. 노션 전송 시작...")
        for visit in final_visits:
            # 중복 체크하면서 전송 (이미 등록되면 existing_ranges에 추가되어 다음 파일 처리 때도 방어됨)
            send_to_notion(visit, existing_ranges, name_tag_memory)

    print(f"\n🎉 모든 작업 완료!")

if __name__ == "__main__":
    main()
