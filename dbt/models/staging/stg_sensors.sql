-- One row per sensor: the sensor -> location bridge. Measurement payloads
-- carry no ids, so this inventory (embedded sensors[] in the locations
-- pages) is what ties stg_measurements back to a station and its metadata.

with location_pages as (

    select
        raw_payload,
        ingested_at
    from {{ source('openaq_raw', 'raw_measurements') }}
    where source_uri like '%/locations.json'

),

location_records as (

    select
        record,
        ingested_at
    from location_pages,
        unnest(json_query_array(raw_payload, '$.results')) as record

),

sensors as (

    select
        cast(json_value(sensor, '$.id') as int64) as sensor_id,
        json_value(sensor, '$.parameter.name') as parameter,
        json_value(sensor, '$.parameter.units') as unit,
        cast(json_value(record, '$.id') as int64) as location_id,
        ingested_at
    from location_records,
        unnest(json_query_array(record, '$.sensors')) as sensor

)

select *
from sensors
qualify row_number() over (partition by sensor_id order by ingested_at desc) = 1
