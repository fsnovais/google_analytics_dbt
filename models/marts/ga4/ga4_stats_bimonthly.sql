{{-
    config(
        materialized = "table"
    )
}}

{%- set metrics = [
    'organic_impressions', 'pageviews', 'sessions'
    , 'engaged_sessions', 'engagement_rate', 'engagement_time_seconds'
    , 'bounces', 'bounces_rate', 'total_conversions'
] %}

with stats_daily as (
    select
        *,
        date_trunc(date, isoweek) as week_date
    from {{ ref('ga4_stats') }}
)
,stats_monthly_pre as (
    select
        site
        , hostname
        , case when
            cast(mod(extract(week from week_date),2) as bool)
            then date_trunc(date_add(week_date, interval 1 week), isoweek)  + interval 6 day
          else date_trunc(week_date, isoweek) + interval 6 day
        end as week_end_dt
        , extract(year from date) as year
        -- , case when
        --     cast(mod(extract(week from week_date),2) as bool)
        --     then extract(week from (date_add(week_date,interval 1 week)))
        --   else extract(week from week_date)
        -- end as week_number
        , case when
            cast(mod(extract(isoweek from week_date),2) as bool)
            then extract(isoweek from (date_add(week_date,interval 1 week)))
          else extract(isoweek from week_date)
        end as isoweek_number
        , landing_page
        , language
        , geo_country as country
        , site_language
        , page_type
        , site_section
        , content_type
        , source
        , medium
        , device_type
        , age
        , gender

        -- metrics
    {%- for metric in metrics %}
        , sum({{ metric }}) as {{ metric }}
    {%- endfor %}
    
    from stats_daily

    group by
        site
        , hostname
        , week_end_dt
        , year
        -- , week_number
        , isoweek_number
        , landing_page
        , language
        , geo_country
        , site_language
        , page_type
        , site_section
        , content_type
        , source
        , medium
        , device_type
        , age
        , gender
)
, final as (
    select
        site
        , hostname
        , week_end_dt
        , year
        , isoweek_number
        , landing_page
        , language
        , country
        , site_language
        , page_type
        , site_section
        , content_type
        , source
        , medium
        , device_type
        , age
        , gender

        -- metrics
    {%- for metric in metrics %}
        , {{ metric }}
        , ifnull({{ metric }} - lag({{ metric }}) over wow, 0) as {{ metric }}_wow
        , ifnull(safe_divide(({{ metric }} - lag({{ metric }}) over wow), lag({{ metric }}) over wow), 0) as {{ metric }}_wow_pct
        , ifnull({{ metric }} - sum({{ metric }}) over yoy, 0) as {{ metric }}_yoy
        , ifnull(safe_divide(({{ metric }} - sum({{ metric }}) over yoy), sum({{ metric }}) over yoy), 0) as {{ metric }}_yoy_pct
    {% endfor -%}

    from stats_monthly_pre

    window
        wow as (
            partition by
                landing_page
                , country
                , source
                , medium
                , device_type
            order by
                year asc
                , isoweek_number asc
                , country asc
                , source asc
                , medium asc
                , device_type asc
        )
        , yoy as (
            partition by
                year
                , isoweek_number
                , landing_page
                , country
                , source
                , medium
                , device_type
            order by
                year asc
                , isoweek_number asc
                , country asc
                , source asc
                , medium asc
                , device_type asc
            ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING
        )
)

select * from final
