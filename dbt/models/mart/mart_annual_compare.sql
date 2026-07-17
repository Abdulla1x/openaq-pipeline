-- Annual means vs WHO annual guidelines. Grain: (country_code, parameter,
-- measurement_year).
--
-- G6: this separate model exists because annual-mean thresholds may only be
-- compared to annual aggregates — never to a single day in
-- mart_country_compare. The annual mean is the mean of station-day averages,
-- so stations weigh by their days of data, not their reporting frequency.
-- G7/G8: days_with_data and locations_with_data make thin years visible —
-- until the Phase 5 backfill, a "year" here is only the ingested days.

with station_days as (

    select * from {{ ref('int_daily_aqi') }}

),

annual_thresholds as (

    select
        pollutant,
        threshold_value,
        unit
    from {{ ref('who_thresholds') }}
    where averaging_period = 'annual'

),

by_country_year as (

    select
        country_code,
        parameter,
        unit,
        extract(year from measurement_date) as measurement_year,
        avg(daily_avg) as annual_mean,
        count(distinct measurement_date) as days_with_data,
        count(distinct location_id) as locations_with_data,
        sum(reading_count) as reading_count
    from station_days
    group by country_code, parameter, unit, measurement_year

)

select
    by_country_year.*,
    annual_thresholds.threshold_value as threshold_annual,
    by_country_year.annual_mean > annual_thresholds.threshold_value
        as exceeds_annual
from by_country_year
left join annual_thresholds
    on
        by_country_year.parameter = annual_thresholds.pollutant
        and by_country_year.unit = annual_thresholds.unit
