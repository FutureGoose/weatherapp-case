with staged as (
    select *
    from {{ ref('stg_openweather') }}
), ranked as (
    select
        location_id,
        location_key,
        location_name,
        latitude,
        longitude,
        event_ts_epoch,
        event_ts_utc,
        event_date,
        ingestion_ts,
        ingestion_ts_raw,
        source_system,
        payload,
        row_number() over (
            partition by location_id, event_date
            order by ingestion_ts desc nulls last, ingestion_ts_raw desc
        ) as row_num
    from staged
)
select
    location_id,
    location_key,
    location_name,
    latitude,
    longitude,
    event_ts_utc,
    event_ts_epoch,
    event_date,
    ingestion_ts,
    ingestion_ts_raw,
    source_system,
    payload
from ranked
where row_num = 1

