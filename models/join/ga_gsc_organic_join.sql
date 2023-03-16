select
    date,
    month_date,
    unix_month_date,
    site,
    site_domain_subfolder,
    hostname,
    landing_page_url,
    site_section,
    country,
    source,
    medium,
    devicecategory,
    first_value(landing_page_url) over (
        partition by month_date, keyword_text order by clicks desc
    ) keyword_top_landing_page,
    keyword_text,
    keyword_intent,
    keyword_category,
    keyword_subcategory,
    keyword_brand,
    page_type,
    impressions,
    clicks,
    avg_position,
    ctr,
    url_bounces,
    engaged_sessions,
    engagement_time_seconds,
    case
        when url_clicks is null
        then url_sessions
        when url_clicks > 0
        then clicks / url_clicks * url_sessions
        else null
    end as keyword_attributed_sessions,
    case
        when url_clicks is null
        then url_pageviews
        when url_clicks > 0
        then clicks / url_clicks * url_pageviews
        else null
    end as keyword_attributed_pageviews,
    case
        when url_clicks is null
        then url_conversions
        when url_clicks > 0
        then clicks / url_clicks * url_conversions
        else null
    end as keyword_attributed_conversions

from
    (
        select
            coalesce(ga360.date, gsc.date) date,
            coalesce(ga360.month_date, gsc.month_date) month_date,
            coalesce(ga360.unix_month_date, gsc.unix_month_date) unix_month_date,
            coalesce(ga360.site, gsc.site) site,
            coalesce(
                ga360.site_domain_subfolder, gsc.site_domain_subfolder
            ) site_domain_subfolder,
            coalesce(ga360.hostname, gsc.page_hostname) hostname,
            coalesce(ga360.landing_page_url, gsc.landing_page_url) landing_page_url,
            coalesce(ga360.site_section, gsc.site_section) site_section,
            coalesce(ga360.country, gsc.country) country,
            coalesce(ga360.source, "Unmapped") source,
            coalesce(ga360.medium, "Unmapped") medium,
            coalesce(ga360.devicecategory, "Unmapped") devicecategory,
            coalesce(gsc.query, "Not Attributed") keyword_text,
            coalesce(gsc.keyword_intent, "Unmapped") keyword_intent,
            coalesce(gsc.keyword_category, "Unmapped") keyword_category,
            coalesce(gsc.keyword_subcategory, "Unmapped") keyword_subcategory,
            coalesce(gsc.keyword_brand, "Unmapped") keyword_brand,
            coalesce(gsc.page_type, "Unmapped") page_type,
            gsc.impressions,
            gsc.clicks,
            sum(clicks) over w1 url_clicks,
            gsc.avg_position,
            gsc.ctr,
            ga360.sessions url_sessions,
            ga360.pageviews url_pageviews,
            ga360.conversions url_conversions,
            ga360.conversions url_bounces,
            ga360.engaged_sessions,
            ga360.engagement_time_seconds
        from {{ ref("ga_360_monthly_agg") }} ga360
        full outer join
            {{ ref("gsc_keyword_mappings") }} gsc
            on ga360.landing_page_url = gsc.landing_page_url
            and ga360.country = gsc.country
            and ga360.month_date = gsc.month_date
        window w1 as (partition by gsc.month_date, gsc.landing_page_url, gsc.country)
    )
