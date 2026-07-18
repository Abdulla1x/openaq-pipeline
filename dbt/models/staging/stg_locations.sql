-- One row per OpenAQ location (monitoring station), deduped to the latest
-- ingested copy: every ingest run re-lands the country's full locations
-- inventory (locations.json pages), so location metadata recurs per day and
-- per rerun batch.

with location_pages as (

    select
        raw_payload,
        ingested_at
    from {{ source('openaq_raw', 'raw_measurements') }}
    where source_uri like '%/locations.json'

),

records as (

    select
        record,
        ingested_at
    from location_pages,
        unnest(json_query_array(raw_payload, '$.results')) as record

),

parsed as (

    select
        cast(json_value(record, '$.id') as int64) as location_id,
        json_value(record, '$.name') as location_name,
        json_value(record, '$.locality') as locality,
        json_value(record, '$.country.code') as country_code,
        -- Reference-grade government monitor vs low-cost sensor: the
        -- instrumentation-quality dimension of the UAE-vs-PK coverage gap.
        cast(json_value(record, '$.isMonitor') as bool) as is_reference_monitor,
        json_value(record, '$.provider.name') as provider_name,
        cast(json_value(record, '$.coordinates.latitude') as float64) as latitude,
        cast(json_value(record, '$.coordinates.longitude') as float64) as longitude,
        json_value(record, '$.timezone') as timezone,
        -- Station activity range as reported by the API (null when a station
        -- has never reported). Drives the Phase 5 history gap audit's
        -- explained-gap classes: a missing station-day before datetime_first
        -- or after datetime_last is expected, not a pipeline gap.
        timestamp(json_value(record, '$.datetimeFirst.utc')) as datetime_first_utc,
        timestamp(json_value(record, '$.datetimeLast.utc')) as datetime_last_utc,
        ingested_at
    from records

)

select *
from parsed
qualify row_number() over (partition by location_id order by ingested_at desc) = 1
