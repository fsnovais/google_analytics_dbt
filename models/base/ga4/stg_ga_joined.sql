{{ config(materialized="table") }}

with
    ga4 as (
        select
            event_date_dt,
            device_category,
            device_mobile_model_name,
            device_operating_system,
            device_operating_system_version,
            -- ,device_browser
            -- ,device_browser_version
            device_language,
            geo_continent,
            geo_country,
            geo_region,
            geo_city,
            geo_sub_continent,
            geo_metro,
            traffic_source_name,
            traffic_source_medium,
            traffic_source_source,
            page_title,
            page_referrer
        from {{ ref("stg_ga4_events_base") }}
    ),
    ua as (
        select
            session_date_dt,
            devicecategory,
            mobiledevicemodel,
            operatingsystem,
            operatingsystemversion,
            -- ,browser
            -- ,browserversion
            language,
            continent,
            country,
            region,
            city,
            subcontinent,
            metro,
            campaign,
            source,
            medium,
            pagetitle,
            referer
        from {{ ref("stg_ga_sessions_base") }}
    )
select *
from ga4
union all
select *
from ua
