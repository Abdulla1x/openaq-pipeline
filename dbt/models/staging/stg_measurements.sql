-- One row per sensor measurement (hourly, period.label = 'raw').
--
-- Measurement payloads carry no sensor/location ids (verified live), so
-- identity is parsed from source_uri: raw/openaq/{COUNTRY}/{date}/{sensor_id}.json.
-- G4: the raw table is append-only; reruns land duplicate pages, so the last
-- CTE keeps the latest ingested copy of each (sensor_id, period_start_utc).
-- A sensor measures exactly one parameter at one location, so this dedup key
-- is equivalent to G4's (location_id, parameter, measurement_ts).

with measurement_pages as (

    select
        raw_payload,
        ingested_at,
        source_uri
    from {{ source('openaq_raw', 'raw_measurements') }}
    where source_uri not like '%/locations.json'

),

records as (

    select
        cast(regexp_extract(source_uri, r'/(\d+)\.json$') as int64) as sensor_id,
        regexp_extract(source_uri, r'/raw/openaq/([A-Z]{2})/') as country_code,
        record,
        ingested_at,
        source_uri
    from measurement_pages,
        unnest(json_query_array(raw_payload, '$.results')) as record

),

parsed as (

    select
        sensor_id,
        country_code,
        json_value(record, '$.parameter.name') as parameter,
        json_value(record, '$.parameter.units') as unit,
        cast(json_value(record, '$.value') as float64) as measurement_value,
        timestamp(json_value(record, '$.period.datetimeFrom.utc')) as period_start_utc,
        timestamp(json_value(record, '$.period.datetimeTo.utc')) as period_end_utc,
        json_value(record, '$.period.label') as period_label,
        json_value(record, '$.period.interval') as period_interval,
        cast(json_value(record, '$.coverage.observedCount') as int64) as observed_count,
        cast(json_value(record, '$.coverage.percentCoverage') as float64) as percent_coverage,
        ingested_at,
        source_uri
    from records

)

select *
from parsed
qualify
    row_number() over (
        partition by sensor_id, period_start_utc
        order by ingested_at desc
    ) = 1
