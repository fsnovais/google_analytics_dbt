{{ config(materialized="table") }}


select
    site,
    site_language,
    geo_country,
    language,
    page_type,
    site_section,
    content_type,
    source,
    medium,
    device_type,
    age,
    gender,
    date,
    hostname,
    landing_page,
    organic_impressions,
    pageviews,
    sessions,
    engaged_sessions,
    engagement_rate,
    engagement_time_seconds,
    bounces,
    bounces_rate,
    total_conversions,
    'GA_4' as data_source
from {{ ref("ga4_stats") }}
union all
select
    trim(split(site, '-')[safe_ordinal(1)]) site_split,
    site,
    country,
    trim(split(site, '-')[safe_ordinal(2)]) language_split,
    page_type,
    site_section,
    null,
    source,
    medium,
    devicecategory,
    null,
    null,
    date,
    hostname,
    keyword_top_landing_page,
    impressions,
    pageviews,
    sessions,
    engaged_sessions,
    engagement_rate,
    engagement_time_seconds,
    bounces,
    bounces_rate,
    conversions,
    'GA_360' as data_source
from {{ ref("agg_keyword_analytics") }}
where date < DATE("2022-08-01")

