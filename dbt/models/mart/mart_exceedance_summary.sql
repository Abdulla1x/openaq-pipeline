-- Days-based exceedance summary, one row per (country_code, parameter).
--
-- G8's originally-drafted days-based rate, realized in Phase 6 as the
-- serving mart: days_exceeded / days_comparable, both exposed as columns —
-- never a rate without its denominator. Sourced from mart_country_compare
-- so the threshold join (pollutant AND unit, G6) and the country-day
-- average are defined exactly once; a country-day with no matching 24h
-- threshold leaves the denominator, never the table.
-- G7: no completeness filter; first/last_day_with_data expose each series'
-- span instead. The *_common columns restrict the rate to the parameter's
-- common window (the latest first-day across countries) because AE's
-- history starts 11 months before PK's — a full-span comparison silently
-- hands AE two winters and PK one. A common window only exists where BOTH
-- countries report the parameter: pm10 is AE-only in practice (PK's six
-- pm10 sensors delivered no in-span data), and a "common" rate for one
-- country would be a misnomer, so its *_common columns are null — absence
-- stays loud (G7) instead of degenerating into a copy of the full span.

with country_days as (

    select * from {{ ref('mart_country_compare') }}

),

country_first_days as (

    select
        country_code,
        parameter,
        min(measurement_date) as first_day
    from country_days
    group by country_code, parameter

),

common_spans as (

    -- count(*) at (country, parameter) grain = countries reporting the
    -- parameter; fewer than 2 means there is nothing to hold in common.
    select
        parameter,
        if(count(*) >= 2, max(first_day), null) as common_span_start
    from country_first_days
    group by parameter

),

summary as (

    select
        country_days.country_code,
        country_days.parameter,
        country_days.unit,
        any_value(country_days.threshold_24h) as threshold_24h,
        min(country_days.measurement_date) as first_day_with_data,
        max(country_days.measurement_date) as last_day_with_data,
        count(*) as days_with_data,
        countif(country_days.threshold_24h is not null) as days_comparable,
        countif(country_days.country_daily_avg > country_days.threshold_24h)
            as days_exceeded,
        avg(country_days.country_daily_avg) as mean_country_daily_avg,
        sum(country_days.locations_exceeding) as station_days_exceeded,
        sum(country_days.locations_comparable) as station_days_comparable,
        any_value(common_spans.common_span_start) as common_span_start,
        countif(
            country_days.threshold_24h is not null
            and country_days.measurement_date >= common_spans.common_span_start
        ) as days_comparable_common,
        countif(
            country_days.country_daily_avg > country_days.threshold_24h
            and country_days.measurement_date >= common_spans.common_span_start
        ) as days_exceeded_common
    from country_days
    inner join common_spans
        on country_days.parameter = common_spans.parameter
    group by country_days.country_code, country_days.parameter, country_days.unit

)

-- With a null common_span_start the countifs above yield 0, which would
-- read as "zero comparable days" — a claim about a window that does not
-- exist. Null the counts too: no window, no numbers.
--
-- The rate below is null in that case as well, but by a different mechanism:
-- a select-list alias is not visible to its sibling expressions in BigQuery,
-- so safe_divide reads summary's raw 0/0 rather than the nulled aliases, and
-- safe_divide(0, 0) is null. Correct, but load-bearing on that identity —
-- anything that replaces the null guard with a sentinel must revisit this
-- line. Verified live on AE pm10 (the one parameter with no common window).
select
    * except (days_comparable_common, days_exceeded_common),
    if(common_span_start is null, null, days_comparable_common)
        as days_comparable_common,
    if(common_span_start is null, null, days_exceeded_common)
        as days_exceeded_common,
    safe_divide(days_exceeded, days_comparable) * 100
        as days_exceedance_rate,
    safe_divide(station_days_exceeded, station_days_comparable) * 100
        as station_days_exceedance_rate,
    safe_divide(days_exceeded_common, days_comparable_common) * 100
        as days_exceedance_rate_common
from summary
