from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pendulum
from datetime import timedelta
import io, gzip, json, requests
from google.cloud import storage

# ---------------- Config ----------------
LOCATIONS = [
    {"lat": "54.6778816", "lon": "-5.9249199"},
    {"lat": "52.6362",   "lon": "-1.1331969"},
    {"lat": "51.456659", "lon": "-0.9696512"},
    {"lat": "54.1775283","lon": "-6.337506"},
    {"lat": "51.4867",   "lon": "0.2433"},
    {"lat": "53.4071991","lon": "-2.99168"},
    {"lat": "53.3045372","lon": "-1.1028469453936067"},
]

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2025, 9, 27, tz="UTC"),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


GCS_BUCKET = "{{ var.value.openweather_gcs_bucket }}"
PARTITION_PREFIX = "openweather/dt={{ ds }}/batch_ts={{ ts_nodash[:15] }}"

# ---------------- Tasks ----------------
def extract_to_gcs(api_key: str, bucket_name: str, prefix: str, **_):
    """
    Fetch OpenWeather for LOCATIONS, write NDJSON lines into chunked gzip parts
    under the given GCS 'prefix', then return that prefix via XCom.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # streaming writer (bounded memory; easy to scale if LOCATIONS grows a lot)
    max_records_per_part = 2  # 50_000
    part_idx = 0
    count_in_part = 0
    buf = io.BytesIO()
    gz = gzip.GzipFile(fileobj=buf, mode="wb")

    def flush_part():
        nonlocal buf, gz, part_idx, count_in_part
        gz.close()
        data = buf.getvalue()
        object_name = f"{prefix}/part-{part_idx:04d}.jsonl.gz"
        blob = bucket.blob(object_name)
        blob.content_encoding = "gzip"
        blob.upload_from_string(data, content_type="application/json")
        part_idx += 1
        count_in_part = 0
        buf = io.BytesIO()
        gz = gzip.GzipFile(fileobj=buf, mode="wb")
        # close the buffer, not manually, with open

    # build & write records one-by-one
    ingestion_ts = pendulum.now("UTC").isoformat()
    session = requests.Session()
    for i, loc in enumerate(LOCATIONS):
        params = {"lat": loc["lat"], "lon": loc["lon"], "appid": api_key}
        resp = session.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params=params,
            timeout=(5, 20),
        )
        resp.raise_for_status()
        payload = resp.json()
        coord = (payload or {}).get("coord") or {}
        record = {
            "ingestion_ts": ingestion_ts,
            "source": "openweather",
            "event_ts": payload.get("dt"),
            "location": {
                "id": payload.get("id"),
                "name": payload.get("name"),
                "lat": coord.get("lat"),
                "lon": coord.get("lon"),
                "key": f"loc_{i}",
            },
            "payload": payload,
        }
        line = json.dumps(record, separators=(",", ":")) + "\n"
        gz.write(line.encode("utf-8"))
        count_in_part += 1
        if count_in_part >= max_records_per_part:
            flush_part()

    if count_in_part > 0:
        flush_part()

    # optional marker to signal "batch complete"
    bucket.blob(f"{prefix}/_SUCCESS").upload_from_string(b"", content_type="text/plain")

    prefix_for_snowflake = prefix.replace("openweather/", "")
    return prefix_for_snowflake  # tiny string -> XCom

with DAG(
    "openweather_gcs_snowflake_chunked",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
) as dag:

    api_key = "{{ conn.openweather.login if conn.openweather else '' }}"
    extract = PythonOperator(
        task_id="extract_to_gcs",
        python_callable=extract_to_gcs,
        op_kwargs={
            "api_key": api_key,
            "bucket_name": GCS_BUCKET,
            "prefix": PARTITION_PREFIX,
        },
    )

    list_batch = SnowflakeOperator(
        task_id="list_batch",
        snowflake_conn_id="snowflake_conn",
        sql="""
        LIST @WEATHER.RAW.STG_OPENWEATHER/{{ ti.xcom_pull(task_ids='extract_to_gcs') }}/;
        """,
    )

    load_to_snowflake = SnowflakeOperator(
        task_id="load_to_snowflake",
        snowflake_conn_id="snowflake_conn",
        sql="""
        COPY INTO WEATHER.RAW.RAW_OPENWEATHER
          FROM @WEATHER.RAW.STG_OPENWEATHER/{{ ti.xcom_pull(task_ids='extract_to_gcs') }}/
          PATTERN='.*\\.jsonl(\\.gz)?$'
          FILE_FORMAT=(FORMAT_NAME=WEATHER.RAW.FF_OPENWEATHER_JSONL)
          ON_ERROR='ABORT_STATEMENT'
          FORCE=FALSE;
        """,
    )

    extract >> load_to_snowflake
