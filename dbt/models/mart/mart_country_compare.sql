-- UAE vs Pakistan, one row per (country_code, parameter, measurement_date).
--
-- G6: daily averages are compared to 24h thresholds only (annual guidelines
-- live in mart_annual_compare at the annual grain).
-- G8: exceedance_rate carries its denominator explicitly —
-- locations_exceeding / locations_with_data, both exposed as columns.
-- G7: no completeness filter; avg_hours_covered rides along so the dashboard
-- can show how thin the data behind a rate is (the UAE-vs-PK coverage gap is
-- itself the finding).
--
-- The threshold join matches on unit as well as pollutant: a value may only
-- be compared to a guideline expressed in its own unit (CO is mg/m³). A
-- station-day with no matching 24h threshold keeps exceeded_24h = null and
-- is excluded from the rate's denominator, never silently dropped.

with station_days as (

    select * from {{ ref('int_daily_aqi') }}

),

thresholds_24h as (

    select
        pollutant,
        threshold_value,
        unit
    from {{ ref('who_thresholds') }}
    where averaging_period = '24h'

),

flagged as (

    select
        station_days.*,
        thresholds_24h.threshold_value as threshold_24h,
        station_days.daily_avg > thresholds_24h.threshold_value as exceeded_24h
    from station_days
    left join thresholds_24h
        on
            station_days.parameter = thresholds_24h.pollutant
            and station_days.unit = thresholds_24h.unit

),

by_country_day as (

    select
        country_code,
        parameter,
        unit,
        measurement_date,
        any_value(threshold_24h) as threshold_24h,
        avg(daily_avg) as country_daily_avg,
        count(distinct location_id) as locations_with_data,
        countif(exceeded_24h) as locations_exceeding,
        countif(exceeded_24h is not null) as locations_comparable,
        sum(reading_count) as reading_count,
        avg(hours_covered) as avg_hours_covered,
        countif(is_reference_monitor) as reference_monitor_locations
    from flagged
    group by country_code, parameter, unit, measurement_date

)

select
    *,
    safe_divide(locations_exceeding, locations_comparable) * 100
        as exceedance_rate,
    avg(country_daily_avg) over (
        partition by country_code, parameter
        order by measurement_date
        rows between 6 preceding and current row
    ) as rolling_7d_avg
from by_country_day
