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
    select * from {{ ref('ga_joined') }}
)
, stats_monthly_pre as (
    select
        site
        , hostname
        , format_date('%Y-%m', date) as year_month
        , extract(year from date) as year
        , extract(month from date) as month_number
        , landing_page
        , language
        , geo_country as country
        -- , site_language
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
        , year_month
        , year
        , month_number
        , landing_page
        , language
        , geo_country
        -- , site_language
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
        , year_month
        , year
        , month_number
        , landing_page
        , language
        , country
        -- , site_language
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
        , ifnull({{ metric }} - lag({{ metric }}) over mom, 0) as {{ metric }}_mom
        , ifnull(safe_divide(({{ metric }} - lag({{ metric }}) over mom), lag({{ metric }}) over mom), 0) as {{ metric }}_mom_pct
        , ifnull({{ metric }} - sum({{ metric }}) over yoy, 0) as {{ metric }}_yoy
        , ifnull(safe_divide(({{ metric }} - sum({{ metric }}) over yoy), sum({{ metric }}) over yoy), 0) as {{ metric }}_yoy_pct
    {% endfor -%}

    from stats_monthly_pre

    window
        mom as (
            partition by
                landing_page
                , country
                , source
                , medium
                , device_type
            order by
                year_month asc
                , country asc
                , source asc
                , medium asc
                , device_type asc
        )
        , yoy as (
            partition by
                year_month
                , landing_page
                , country
                , source
                , medium
                , device_type
            order by
                year_month asc
                , country asc
                , source asc
                , medium asc
                , device_type asc
            ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING
        )
)

select * from final
