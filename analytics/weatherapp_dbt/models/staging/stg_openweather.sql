with raw as (
    select data
    from WEATHER.RAW.RAW_OPENWEATHER
),
flattened as (
    select
        data:source::string             as source_system,
        data:ingestion_ts::string       as ingestion_ts_raw,
        try_to_timestamp(data:ingestion_ts::string) as ingestion_ts,
        data:event_ts::number           as event_ts_epoch,
        to_timestamp(data:event_ts::number)         as event_ts_utc,
        date(to_timestamp(data:event_ts::number))   as event_date,
        data:location:id::number        as location_id,
        data:location:key::string       as location_key,
        data:location:name::string      as location_name,
        data:location:lat::float        as latitude,
        data:location:lon::float        as longitude,
        data:payload                    as payload
    from raw
)
select *
from flattened

