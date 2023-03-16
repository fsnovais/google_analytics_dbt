{{- config(materialized="table") }}

{%- set metrics = [
    "organic_impressions",
    "pageviews",
    "sessions",
    "engaged_sessions",
    "engagement_rate",
    "engagement_time_seconds",
    "bounces",
    "bounces_rate",
    "total_conversions",
] %}

with
    stats_daily as (select * from {{ ref("ga_joined") }}),
    stats_monthly_pre as (
        select
            site,
            hostname,
            date_trunc(date, isoweek) + interval 6 day as week_end_dt,
            extract(year from date) as year,
            -- , extract(week from date) as week_number -- it can't be used along
            -- isoweek_number because of agg
            extract(isoweek from date) as isoweek_number,
            landing_page,
            language,
            geo_country as country,
            site_language,
            page_type,
            site_section,
            content_type,
            source,
            medium,
            device_type,
            age,
            gender

            -- metrics
            {%- for metric in metrics %}
            , sum({{ metric }}) as {{ metric }}
            {%- endfor %}

        from stats_daily

        group by
            site,
            hostname,
            week_end_dt,
            year,
            -- , week_number
            isoweek_number,
            landing_page,
            language,
            geo_country,
            site_language,
            page_type,
            site_section,
            content_type,
            source,
            medium,
            device_type,
            age,
            gender
    ),
    final as (
        select
            site,
            hostname,
            week_end_dt,
            year,
            isoweek_number,
            landing_page,
            language,
            country,
            site_language,
            page_type,
            site_section,
            content_type,
            source,
            medium,
            device_type,
            age,
            gender

            -- metrics
            {%- for metric in metrics %}
            ,
            {{ metric }},
            ifnull({{ metric }} - lag({{ metric }}) over wow, 0) as {{ metric }}_wow,
            ifnull(
                safe_divide(
                    ({{ metric }} - lag({{ metric }}) over wow),
                    lag({{ metric }}) over wow
                ),
                0
            ) as {{ metric }}_wow_pct,
            ifnull({{ metric }} - sum({{ metric }}) over yoy, 0) as {{ metric }}_yoy,
            ifnull(
                safe_divide(
                    ({{ metric }} - sum({{ metric }}) over yoy),
                    sum({{ metric }}) over yoy
                ),
                0
            ) as {{ metric }}_yoy_pct
            {% endfor -%}

        from stats_monthly_pre

        window
            wow as (
                partition by landing_page, country, source, medium, device_type
                order by
                    year asc,
                    isoweek_number asc,
                    country asc,
                    source asc,
                    medium asc,
                    device_type asc
            ),
            yoy as (
                partition by
                    year,
                    isoweek_number,
                    landing_page,
                    country,
                    source,
                    medium,
                    device_type
                order by
                    year asc,
                    isoweek_number asc,
                    country asc,
                    source asc,
                    medium asc,
                    device_type asc
                rows between 11 preceding and 1 preceding
            )
    )

select *
from
    final
    -- where landing_page = 'askgamblers.com/online-casinos/reviews/betway-casino'
    -- and isoweek_number = 35
    -- and country = 'Canada'
    -- and device_type = 'mobile'
    -- and source = 'google'
    
