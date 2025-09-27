from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import json
import gzip
import io

LOCATIONS = [
    {"lat": "54.6778816", "lon": "-5.9249199"},
    {"lat": "52.6362", "lon": "-1.1331969"},
    {"lat": "51.456659", "lon": "-0.9696512"},
    {"lat": "54.1775283", "lon": "-6.337506"},
    {"lat": "51.4867", "lon": "0.2433"},
    {"lat": "53.4071991", "lon": "-2.99168"},
    {"lat": "53.3045372", "lon": "-1.1028469453936067"},
]

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('openweather_to_gcs', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    @task
    def fetch_weather(api_key: str, execution_date: str) -> dict:
        import requests
        results = {}
        for i, loc in enumerate(LOCATIONS):
            params = { 'lat': loc['lat'], 'lon': loc['lon'], 'appid': api_key }
            resp = requests.get('https://api.openweathermap.org/data/2.5/weather', params=params)
            resp.raise_for_status()
            results[f'loc_{i}'] = resp.json()
        return results

    @task
    def upload_to_gcs(data: dict, gcs_bucket: str, execution_date: str) -> str:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(gcs_bucket)

        # Build NDJSON payload (raw/bronze): minimal metadata + full payload
        lines = []
        ingestion_ts = datetime.utcnow().isoformat()
        for key, payload in data.items():
            if not isinstance(payload, dict):
                continue
            coord = payload.get('coord') or {}
            record = {
                'ingestion_ts': ingestion_ts,
                'source': 'openweather',
                'event_ts': payload.get('dt'),
                'location': {
                    'id': payload.get('id'),
                    'name': payload.get('name'),
                    'lat': coord.get('lat'),
                    'lon': coord.get('lon'),
                    'key': key,
                },
                'payload': payload,
            }
            lines.append(json.dumps(record, separators=(',', ':')))

        ndjson = "\n".join(lines) + ("\n" if lines else "")

        # Gzip-compress
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
            gz.write(ndjson.encode('utf-8'))
        content = buffer.getvalue()

        # Partition by date, write jsonl.gz
        safe_ts = ingestion_ts.replace('-', '').replace(':', '').split('.', 1)[0]
        path = f"openweather/dt={execution_date}/batch_ts={safe_ts}/part-0000.jsonl.gz"
        blob = bucket.blob(path)
        blob.content_encoding = 'gzip'
        blob.upload_from_string(content, content_type='application/json')
        return path

    api_key = '{{ conn.openweather.login if conn.openweather else "" }}'
    gcs_bucket = '{{ var.value.openweather_gcs_bucket }}'
    execution_date = '{{ ds }}'

    data = fetch_weather(api_key=api_key, execution_date=execution_date)
    gcs_path = upload_to_gcs(data=data, gcs_bucket=gcs_bucket, execution_date=execution_date)


"""
NDJSON vs JSON: NDJSON is preferred for raw lake dumps. It’s append‑friendly, streamable, 
resilient to partial writes, and Snowflake ingests it line‑by‑line cleanly.


"""