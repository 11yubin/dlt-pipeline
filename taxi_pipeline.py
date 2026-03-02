import dlt
import requests

# 1. API 데이터 추출을 위한 제너레이터 함수 정의
@dlt.resource(name="taxi_trips", write_disposition="replace")
def fetch_taxi_data():
    base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    page = 1
    
    while True:
        params = {"page": page}
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # 빈 페이지가 반환되면 중단
        if not data:
            break
            
        yield data
        print(f"Page {page} loaded...")
        page += 1

# 2. 파이프라인 실행 함수
def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="ny_taxi_data",
    )
    
    # 데이터 로드 실행
    load_info = pipeline.run(fetch_taxi_data())
    print(load_info)

if __name__ == "__main__":
    run_pipeline()