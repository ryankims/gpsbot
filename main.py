import pandas as pd
import requests
from datetime import datetime, timedelta
from dateutil import parser
import os

# --- 1. 설정 정보 (본인의 정보로 변경하세요) ---
MY_NOTION_KEY = "YOUR_NOTION_INTEGRATION_KEY"
MY_NOTION_DB_ID = "YOUR_DATABASE_ID"
MY_TAG_RULES = {
    "식당": "🍴 식사",
    "카페": "☕ 카페",
    "공원": "🌳 산책",
    "회사": "💻 업무"
}

# --- 2. 시간대 제거 후 비교하는 함수 ---
def send_to_notion(v, existing, name_tag_memory):
    # 신규 데이터 시간대 정보 제거 (Naive 변환)
    new_start = v["start"].replace(tzinfo=None) if v["start"].tzinfo else v["start"]
    
    for rec in existing:
        # 기존 데이터 시간대 정보 제거 (Naive 변환)
        ex_start = rec["start"].replace(tzinfo=None) if rec["start"].tzinfo else rec["start"]
        
        # 5분(300초) 이내 중복 체크
        if abs((new_start - ex_start).total_seconds()) < 300:
            print(f"🛡️ [중복] {v['place_name']} (시간: {new_start}) 패스")
            return

    # 태그 결정 로직
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
            "체류시간": {"rich_text": [{"text": {"content": f"{v['duration']}분"}}]},
            "방문일시": {"date": {"start": v["start"].isoformat(), "end": v["end"].isoformat()}},
            "Lat": {"number": v["lat"]},
            "Lon": {"number": v["lon"]},
            "PlaceID": {"rich_text": [{"text": {"content": v["place_id"]}}]}
        }
    }

    resp = requests.post(
        "https://api.notion.com/v1/pages",
        headers={
            "Authorization": f"Bearer {MY_NOTION_KEY}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        },
        json=payload
    )
    if resp.status_code == 200:
        print(f"✅ 등록 완료: {v['place_name']}")
    else:
        print(f"❌ 등록 실패: {v['place_name']} ({resp.status_code})")

# --- 3. 기존 노션 데이터 가져오기 (학습 및 중복 방지) ---
def get_existing_records():
    existing = []
    url = f"https://api.notion.com/v1/databases/{MY_NOTION_DB_ID}/query"
    headers = {
        "Authorization": f"Bearer {MY_NOTION_KEY}",
        "Notion-Version": "2022-06-28"
    }
    
    resp = requests.post(url, headers=headers)
    if resp.status_code == 200:
        results = resp.json().get("results", [])
        for page in results:
            props = page["properties"]
            date_prop = props.get("방문일시", {}).get("date")
            name_prop = props.get("이름", {}).get("title", [])
            
            if date_prop and date_prop["start"]:
                # 가져올 때부터 시간대 정보를 제거하여 통일
                s = parser.parse(date_prop["start"]).replace(tzinfo=None)
                name = name_prop[0]["text"]["content"] if name_prop else ""
                existing.append({"start": s, "name": name})
    return existing

# --- 4. 메인 실행 루프 ---
def main():
    # 1. 기존 데이터 로드
    print("🔍 기존 노션 데이터를 확인 중입니다...")
    existing_data = get_existing_records()
    name_tag_memory = {item["name"]: "이전 기록" for item in existing_data}

    # 2. 처리할 파일 리스트 (파일명은 실제 환경에 맞게 수정하세요)
    csv_files = ["data1.csv", "data2.csv", "data3.csv", "data4.csv", "data5.csv"]

    for file_path in csv_files:
        if not os.path.exists(file_path):
            print(f"⚠️ 파일 없음: {file_path}, 건너뜁니다.")
            continue
            
        print(f"🚀 {file_path} 분석 시작...")
        df = pd.read_csv(file_path)
        
        # CSV의 'start', 'end' 컬럼을 datetime 객체로 변환 (시간대 정보 없음)
        df["start"] = pd.to_datetime(df["start"])
        df["end"] = pd.to_datetime(df["end"])

        for _, row in df.iterrows():
            send_to_notion(row, existing_data, name_tag_memory)

    print("🏁 모든 작업이 완료되었습니다!")

if __name__ == "__main__":
    main()
