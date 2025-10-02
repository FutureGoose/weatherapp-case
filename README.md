weatherapp-case

Minimal setup guide to run the local Airflow pipeline, land OpenWeather data in GCS, and load it into Snowflake.

Prerequisites
- Docker and Docker Compose
- gcloud SDK with gsutil (authenticated to the target GCP project)
- Snowflake account (warehouse, role, and database you can use)

1) First-time setup (one time)
- Create `.env` so Airflow runs as your UID and installs needed providers on startup:
```bash
printf 'AIRFLOW_UID=%s\n_PIP_ADDITIONAL_REQUIREMENTS=requests google-cloud-storage apache-airflow-providers-snowflake\n' "$(id -u)" > .env
```
- Start Airflow:
```bash
docker compose up airflow-init
docker compose up -d
```
- Web UI: http://localhost:8080 (default: airflow / airflow)

2) Common Docker commands
```bash
docker compose up -d
docker compose down
docker compose restart airflow-scheduler airflow-webserver
```

3) GCP bucket and service account
- Grant the Airflow service-account upload rights to the bucket (replace with your values):
```bash
gsutil iam ch serviceAccount:weatherapp-case@team-god.iam.gserviceaccount.com:objectCreator gs://openweathermap_data
# verify
gsutil iam get gs://openweathermap_data | grep weatherapp-case@ -C2
```
- Save the service-account key locally (kept outside git; `keys/` is gitignored):
```bash
gcloud iam service-accounts keys create /home/gustaf/projects/weatherapp-case/keys/gcp-key.json \
  --iam-account=weatherapp-case@team-god.iam.gserviceaccount.com
```
The `docker-compose.yaml` mounts `./keys` and sets `GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/keys/gcp-key.json`.

4) Airflow connections and variables
OpenWeather (API key stored as the connection login):
```bash
docker compose exec airflow-webserver airflow connections add openweather \
  --conn-type http \
  --conn-host https://api.openweathermap.org \
  --conn-login '<YOUR_OPENWEATHER_API_KEY>'

docker compose exec airflow-webserver airflow variables set openweather_gcs_bucket 'openweathermap_data'

# verify / manage
docker compose exec airflow-webserver airflow connections get openweather
docker compose exec airflow-webserver airflow connections list | cat
# docker compose exec airflow-webserver airflow connections delete openweather
```

5) Snowflake setup (Terraform)
- Before triggering the DAG, provision/check the Snowflake database objects with Terraform. Follow `infra/snowflake/README.md` for the exact exports and commands (including granting the Snowflake storage integration access to your GCS bucket).
To run put your credentials in export TF_VAR_snowflake_password=<YOUR_SNOWFLAKE_PASSWORD>.


6) Snowflake connection (Airflow)
You only need to do this once for your Airflow container.
- Replace account/user/password with yours (account identifier only, no domain):
```bash
docker compose exec airflow-webserver airflow connections add snowflake_conn \
  --conn-type snowflake \
  --conn-host 'IFNTRPR-AK19625' \
  --conn-login 'GOOSE' \
  --conn-password '<YOUR_SNOWFLAKE_PASSWORD>' \
  --conn-extra '{"account":"IFNTRPR-AK19625","warehouse":"COMPUTE_WH","database":"WEATHER","schema":"RAW","role":"ACCOUNTADMIN"}'

docker compose exec airflow-webserver airflow connections get snowflake_conn
```

7) Run the pipeline
The DAG fetches current weather for configured locations, writes NDJSON to `gs://openweathermap_data/openweather/dt=YYYY-MM-DD/batch_ts=.../part-0000.jsonl.gz`, then loads the same day’s files into `WEATHER.RAW.RAW_OPENWEATHER` in Snowflake.
```bash
docker compose exec airflow-webserver airflow dags trigger openweather_gcs_snowflake_dag
```

8) Run dbt dedup (curated models)
- See `analytics/weatherapp_dbt/README.md` for full instructions. Minimal:
```bash
export DBT_SNOWFLAKE_PASSWORD='<YOUR_SNOWFLAKE_PASSWORD>'
source .venv/bin/activate
cd analytics/weatherapp_dbt
dbt run -s +fct_daily_openweather
```


Notes
- Keep secrets out of git; `keys/` is already in `.gitignore`.
- If you use SSO in Snowflake, set a programmatic password for the user or switch the connection to `{"authenticator":"externalbrowser"}`.
- COPY in Snowflake de‑duplicates via load history; reruns only ingest new files unless forced (DAG).
