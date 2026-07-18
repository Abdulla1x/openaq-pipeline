-- Station-day aggregates. Grain: (location_id, parameter, measurement_date).
--
-- G7: completeness (reading_count, hours_covered) is a *dimension*, never a
-- filter — low-coverage days stay in and the gap surfaces as a finding.
-- hours_covered counts distinct hours (sensors report at different
-- frequencies, so raw reading counts are not comparable across stations).
-- The day is the UTC date of the measurement period, not the ingest day.
--
-- Valueless records are excluded here, not in staging: the API can serve
-- records with "value": null (seen in the Phase 5 backfill — sensor
-- 13144205, 19 records over four days of 2026-04). A record without a
-- reading is not a reading: counting it would inflate the G7 completeness
-- columns, and a day of only-null records would emit a null daily_avg.
-- Staging keeps such rows (faithful parse; not_null warns there), so the
-- exclusion is visible in exactly one place — this filter.

with measurements as (

    select *
    from {{ ref('stg_measurements') }}
    where measurement_value is not null

),

sensors as (

    select * from {{ ref('stg_sensors') }}

),

locations as (

    select * from {{ ref('stg_locations') }}

)

select
    locations.location_id,
    locations.country_code,
    measurements.parameter,
    measurements.unit,
    date(measurements.period_start_utc) as measurement_date,
    avg(measurements.measurement_value) as daily_avg,
    min(measurements.measurement_value) as daily_min,
    max(measurements.measurement_value) as daily_max,
    count(*) as reading_count,
    count(distinct timestamp_trunc(measurements.period_start_utc, hour))
        as hours_covered,
    count(distinct measurements.sensor_id) as sensor_count,
    locations.is_reference_monitor
from measurements
inner join sensors on measurements.sensor_id = sensors.sensor_id
inner join locations on sensors.location_id = locations.location_id
group by
    locations.location_id,
    locations.country_code,
    measurements.parameter,
    measurements.unit,
    measurement_date,
    locations.is_reference_monitor
