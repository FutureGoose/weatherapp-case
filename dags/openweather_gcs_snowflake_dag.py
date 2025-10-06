from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pendulum
from datetime import timedelta
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
    'start_date': pendulum.datetime(2025, 9, 27, tz='UTC'),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'openweather_gcs_snowflake_dag',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    catchup=False,
) as dag:

    @task
    def fetch_weather(api_key: str, execution_date: str) -> dict:
        import requests
        results = {}
        for i, loc in enumerate(LOCATIONS):
            params = { 'lat': loc['lat'], 'lon': loc['lon'], 'appid': api_key }
            resp = requests.get('https://api.openweathermap.org/data/2.5/weather', params=params, timeout=(5, 20))
            resp.raise_for_status()
            results[f'loc_{i}'] = resp.json()
        return results

    @task
    def upload_to_gcs(data: dict, gcs_bucket: str, execution_date: str) -> str:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(gcs_bucket)

        # build NDJSON payload: minimal metadata + full payload
        lines = []
        ingestion_ts = pendulum.now('UTC').isoformat()
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

        # gzip-compress, for future stream-based chunking
        # buffer = io.BytesIO()
        # with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
        #     gz.write(ndjson.encode('utf-8'))
        # content = buffer.getvalue()

        # simple version
        content = gzip.compress(ndjson.encode('utf-8'))

        # partition by date, write jsonl.gz
        safe_ts = ingestion_ts.replace('-', '').replace(':', '').split('.', 1)[0]
        path = f"openweather/dt={execution_date}/batch_ts={safe_ts}/part-0000.jsonl.gz"
        blob = bucket.blob(path)
        blob.content_encoding = 'gzip'
        blob.upload_from_string(content, content_type='application/json')
        return path

    api_key = '{{ conn.openweather.login if conn.openweather else "" }}'
    gcs_bucket = '{{ var.value.openweather_gcs_bucket }}'
    execution_date = '{{ ds }}'

    fetch_task = fetch_weather(api_key=api_key, execution_date=execution_date)
    upload_task = upload_to_gcs(data=fetch_task, gcs_bucket=gcs_bucket, execution_date=execution_date)

    load_to_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake',
        snowflake_conn_id='snowflake_conn',
        sql=(
            """
            COPY INTO WEATHER.RAW.RAW_OPENWEATHER
              FROM @WEATHER.RAW.STG_OPENWEATHER
              PATTERN='.*\\.jsonl(\\.gz)?$'  -- matches .jsonl or .jsonl.gz
              FILE_FORMAT=(FORMAT_NAME=WEATHER.RAW.FF_OPENWEATHER_JSONL)
              ON_ERROR='ABORT_STATEMENT'     -- no partial loads
              FORCE=FALSE;                   -- only ingest new
            """
        ),
    )
    fetch_task >> upload_task >> load_to_snowflake