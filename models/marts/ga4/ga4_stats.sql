{{ config(materialized="table") }}

with
    ga4_all_sources as (select * from {{ ref("stg_ga4_events_union") }}),
    stg_ga4_events_base as (select * from {{ ref("stg_ga4_events_base") }}),
    site_language_map as (
        -- TODO: create source reference
        select * from `bi-bq-cwv-global.agency_data_pipeline.sites`
    ),
    page_types_map as (select * from {{ ref("stg_page_types") }}),
    base as (
        select
            null as name,
            null as time_of_entry,
            base.site_name as site,
            coalesce(
                slm_url.site_name_language, slm_domain.site_name_language
            ) as site_language,
            -- , coalesce(slm_url.country, slm_domain.country) as site_country
            base.geo_country,
            -- , coalesce(slm_url.language, slm_domain.language) as language
            case
                when contains_substr(base.page_url_trimmed, '/es')
                then 'Spanish'
                when contains_substr(base.page_url_trimmed, '/de')
                then 'German'
                when contains_substr(base.page_url_trimmed, '/it')
                then 'Italian'
                when contains_substr(base.page_url_trimmed, '/ja')
                then 'Japanese'
                when contains_substr(base.page_url_trimmed, '/pt')
                then 'Portuguese'
                when contains_substr(base.page_url_trimmed, '/sv')
                then 'Swedish'
                else 'English'
            end as language,
            case
                when regexp_contains(base.page_url_trimmed, ptm.regex)
                then ptm.page_type
                else null
            end as page_type,
            base.site_section,
            null as content_type,
            source,
            medium,
            device_category as device_type,
            null as age,
            null as gender,
            base.event_date_dt as date,
            base.page_hostname as hostname,
            base.page_url_trimmed as landing_page,
            -- ** Session metrics
            countif(base.traffic_source_medium = 'organic') as organic_impressions,
            countif(is_page_view = 1) as pageviews,
            count(distinct session_key) as sessions,
            count(
                distinct case when session_engaged = 1 then session_key end
            ) as engaged_sessions,
            safe_divide(
                count(distinct case when session_engaged = 1 then session_key end),
                count(distinct session_key)
            ) as engagement_rate,
            sum(engagement_time_seconds) as engagement_time_seconds,
            count(distinct session_key) - count(
                distinct case when session_engaged = 1 then session_key end
            ) as bounces,
            safe_divide(
                count(distinct session_key)
                - count(distinct case when session_engaged = 1 then session_key end),
                count(distinct session_key)
            ) as bounces_rate,
            countif(is_affiliate_link_click = 1) as total_conversions

        from stg_ga4_events_base base

        left outer join
            site_language_map slm_url on slm_url.primary_domain = base.page_url_language

        left outer join
            site_language_map slm_domain
            on slm_domain.primary_domain = base.page_hostname

        left outer join page_types_map ptm on ptm.site = base.site_name

        group by
            base.site_name,
            site_language,
            -- , site_country
            base.geo_country,
            language,
            page_type,
            site_section,
            source,
            medium,
            device_category,
            event_date_dt,
            page_hostname,
            page_url_trimmed
    ),
    final as (
        select
            base.site,
            base.site_language,
            -- , base.site_country
            base.geo_country,
            base.language,
            ifnull(string_agg(distinct base.page_type, ', '), 'Unmapped') as page_type,
            base.site_section,
            base.content_type,
            base.source,
            base.medium,
            base.device_type,
            base.age,
            base.gender,
            base.date,
            base.hostname,
            base.landing_page,
            base.organic_impressions,
            base.pageviews,
            base.sessions,
            base.engaged_sessions,
            base.engagement_rate,
            base.engagement_time_seconds,
            base.bounces,
            base.bounces_rate,
            base.total_conversions

        from base

        group by
            base.site,
            base.site_language,
            -- , base.site_country
            base.geo_country,
            base.language,
            base.site_section,
            base.content_type,
            base.source,
            base.medium,
            base.device_type,
            base.age,
            base.gender,
            base.date,
            base.hostname,
            base.landing_page,
            base.organic_impressions,
            base.pageviews,
            base.sessions,
            base.engaged_sessions,
            base.engagement_rate,
            base.engagement_time_seconds,
            base.bounces,
            base.bounces_rate,
            base.total_conversions

    ),
    hashed as (
        select
            -- TODO: This can be improved by using a combination of "set" and "for
            -- loop" in Jinja templates
            md5(
                concat(date, landing_page, geo_country, source, medium, device_type)
            ) as hash_key,
            array_to_string(
                [
                    'date',
                    'landing_page',
                    geo_country,
                    'source',
                    'medium',
                    'device_type'
                ],
                '; '
            ) as natural_group_columns,
            *
        from final
    )

select *
from hashed
