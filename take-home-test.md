## Data Engineer - Take-Home Test
1. Airflow DAG
Write an Airflow DAG that retrieves daily weather data from the OpenWeather API for the following locations:

```json	
locations = [
{"lat": "54.6778816", "lon": "-5.9249199"},
{"lat": "52.6362", "lon": "-1.1331969"},
{"lat": "51.456659", "lon": "-0.9696512"},
{"lat": "54.1775283", "lon": "-6.337506"},
{"lat": "51.4867", "lon": "0.2433"},
{"lat": "53.4071991", "lon": "-2.99168"},
{"lat": "53.3045372", "lon": "-1.1028469453936067"},
]
```

Store the data in a Google Cloud Storage (GCS) bucket (`gs://<bucket>/...`) in a format suitable for ingestion into Snowflake.

Requirements:
- Follow Airflow and Python best practices.
- Handle cloud credentials securely.
- Write clean, modular, and production-ready code.

2. Architecture Design:

    1. Describe your approach for loading data from cloud storage into Snowflake, considering cost, scalability, and
    latency. Compare different options, and list the pros and cons of each.

    2. Design a high-level plan how you would transform raw data into insights. Consider both data warehouse
    architecture, but also tools and orchestration you would use.

3. Bonus
    - Explain how you ran Airflow for this exercise.
    - If applicable, include any Infrastructure-as-Code scripts (e.g., Terraform, Pulumi, Docker Compose) you used for
setup.