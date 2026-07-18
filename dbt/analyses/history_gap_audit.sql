-- Phase 5 history gap audit (exit criterion: "history validated — no
-- unexplained gaps"). Point-in-time analysis, compiled and run manually
-- (dbt compile → bq query); deliberately NOT a dbt test — gaps are normal
-- and expected (dormant AE stations, PK's onboarding curve), so a must-pass
-- test would be either tautological or flaky.
--
-- Grain: country × parameter × month over the audited spans. For every
-- (sensor, day) expected cell, missing data is classified:
--
--   with_data          — a measurement landed for that sensor-day.
--   pre_onboarding     — day precedes the station's datetime_first (or the
--                        station has never reported): history that does not
--                        exist upstream.
--   post_dormancy      — day follows the station's datetime_last: station
--                        stopped reporting.
--   known_bad          — sensor is in the known_bad_sensors seed: its fetches
--                        fail (or are skipped) server-side, so upstream data
--                        MAY exist that the pipeline could not retrieve. The
--                        only class where the pipeline is the limiting factor.
--   upstream_no_data   — fetched successfully, API returned nothing: station
--                        outage/intermittency between its first/last range.
--                        Explained BY CONSTRUCTION: every backfill chunk is
--                        reconciled (API-side count == BigQuery count), so
--                        whatever the API served for the window is exactly
--                        what landed.
--
-- Residual "unexplained" is therefore structural anomalies only:
-- data_before_first — a landed measurement earlier than the station's
-- declared datetime_first (upstream metadata inconsistency, worth eyeballing,
-- not a pipeline defect). Audit passes when known_bad + data_before_first are
-- small and individually understood.
--
-- Audited spans (probe-driven, 2026-07-18; see PROJECT_CONTEXT §7 Phase 5):
-- AE from 2024-07-01 (2-year cap; reference network live since 2022-10),
-- PK from 2025-06-01 (earliest month with ≥25% of current target sensors —
-- only 9% existed before; the fleet is that young).

with audit_spans as (

    select 'AE' as country_code, date '2024-07-01' as span_start, date '2026-07-17' as span_end
    union all
    select 'PK' as country_code, date '2025-06-01' as span_start, date '2026-07-17' as span_end

),

sensors as (

    select
        s.sensor_id,
        s.parameter,
        l.country_code,
        date(l.datetime_first_utc) as first_day,
        date(l.datetime_last_utc) as last_day
    from {{ ref('stg_sensors') }} as s
    inner join {{ ref('stg_locations') }} as l on s.location_id = l.location_id
    -- Only the parameters the pipeline fetches (TARGET_PARAMETERS): a "gap"
    -- for a sensor we deliberately never call is not a gap.
    where s.parameter in ('pm25', 'pm10', 'no2')

),

expected as (

    select
        sensors.sensor_id,
        sensors.parameter,
        sensors.country_code,
        sensors.first_day,
        sensors.last_day,
        expected_day
    from sensors
    inner join audit_spans on sensors.country_code = audit_spans.country_code,
        unnest(generate_date_array(audit_spans.span_start, audit_spans.span_end))
            as expected_day

),

actual as (

    select distinct
        sensor_id,
        date(period_start_utc) as measurement_day
    from {{ ref('stg_measurements') }}

),

classified as (

    select
        expected.country_code,
        expected.parameter,
        date_trunc(expected.expected_day, month) as audit_month,
        case
            when actual.sensor_id is not null
                and expected.first_day is not null
                and expected.expected_day < expected.first_day
                then 'data_before_first'
            when actual.sensor_id is not null then 'with_data'
            when expected.first_day is null or expected.expected_day < expected.first_day
                then 'pre_onboarding'
            when expected.last_day is not null and expected.expected_day > expected.last_day
                then 'post_dormancy'
            when kb.sensor_id is not null then 'known_bad'
            else 'upstream_no_data'
        end as gap_class
    from expected
    left join actual
        on
            expected.sensor_id = actual.sensor_id
            and expected.expected_day = actual.measurement_day
    left join {{ ref('known_bad_sensors') }} as kb on expected.sensor_id = kb.sensor_id

)

select
    country_code,
    parameter,
    audit_month,
    count(*) as expected_sensor_days,
    countif(gap_class = 'with_data') as with_data,
    countif(gap_class = 'pre_onboarding') as pre_onboarding,
    countif(gap_class = 'post_dormancy') as post_dormancy,
    countif(gap_class = 'known_bad') as known_bad,
    countif(gap_class = 'upstream_no_data') as upstream_no_data,
    countif(gap_class = 'data_before_first') as data_before_first,
    round(countif(gap_class = 'with_data') / count(*) * 100, 1) as coverage_pct
from classified
group by country_code, parameter, audit_month
order by country_code, parameter, audit_month
