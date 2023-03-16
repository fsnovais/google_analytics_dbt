select
    date,
    month_date,
    unix_month_date,
    site,
    site_domain_subfolder,
    hostname,
    site_section,
    country,
    source,
    medium,
    devicecategory,
    keyword_top_landing_page,
    keyword_text,
    keyword_intent,
    keyword_category,
    keyword_subcategory,
    keyword_brand,
    page_type,
    sum(impressions) impressions,
    sum(clicks) clicks,
    avg(avg_position) avg_position,
    sum(keyword_attributed_sessions) sessions,
    sum(keyword_attributed_pageviews) pageviews,
    sum(keyword_attributed_conversions) conversions
from {{ ref("ga_gsc_organic_join") }}
group by
    date,
    month_date,
    unix_month_date,
    site,
    site_domain_subfolder,
    hostname,
    site_section,
    country,
    source,
    medium,
    devicecategory,
    keyword_top_landing_page,
    keyword_text,
    keyword_intent,
    keyword_category,
    keyword_subcategory,
    keyword_brand,
    page_type
