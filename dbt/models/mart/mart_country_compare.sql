-- UAE vs Pakistan, one row per (country_code, parameter, measurement_date).
--
-- G6: daily averages are compared to 24h thresholds only (annual guidelines
-- live in mart_annual_compare at the annual grain).
-- G8: exceedance_rate carries its denominator explicitly —
-- locations_exceeding / locations_comparable, both exposed as columns.
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
    avg(country_daily_avg) over w_7d as rolling_7d_avg,
    -- Ratio of sums, not the mean of the daily rates: AE reports ~8 stations
    -- a day to PK's ~180, so averaging rates would weight a 3-station day
    -- equal to a 180-station one. Summing numerator and denominator across
    -- the window keeps G8's denominator a real quantity -- this is the share
    -- of station-days exceeding over the last 7 calendar days.
    safe_divide(
        sum(locations_exceeding) over w_7d,
        sum(locations_comparable) over w_7d
    ) * 100 as rolling_7d_station_exceedance_rate
from by_country_day
-- range frame over unix_date = 7 *calendar* days; a rows frame would
-- silently widen the window across day gaps in thin coverage (G7).
window w_7d as (
    partition by country_code, parameter
    order by unix_date(measurement_date)
    range between 6 preceding and current row
)
