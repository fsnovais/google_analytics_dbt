{{config(materialized = 'table')}}

WITH 
    union_session AS (
        SELECT
        *,
        _TABLE_SUFFIX
        FROM `seo-ag.183853840.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.105263948.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.165478623.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.175962729.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.225371913.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.179442303.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.200066837.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.200066837.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.77278837.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.176228952.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.183852433.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.176238156.ga_sessions_*`
        UNION ALL
        SELECT
        *,
        _TABLE_SUFFIX
        FROM
        `seo-ag.195930178.ga_sessions_*`
    ),
data as (
	SELECT 
	distinct visitId,
	visitStartTime,
	clientId,
	geoNetwork.country,
	parse_date("%Y%m%d", date) date,
	lower(trim(regexp_replace(regexp_replace(replace(replace(replace(replace(h.appInfo.landingScreenName,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),r'\#.*$',''),'/')) landing_page,
	max(case when h.eventinfo.eventcategory = 'Outbound links' and h.eventinfo.eventaction = 'Click' then 1 else 0 end) affiliate_clicks_conversion,
	max(case when h.eventinfo.eventcategory = 'Outbound links' and h.eventinfo.eventaction = 'Click Top 10' then 1 else 0 end) affiliate_top10_clicks_conversion,
	max(case when h.type = 'PAGE' and h.page.pagePath like '/complaint-received%' and channelgrouping = 'Organic Search' then 1 else 0 end) complaint_received_conversion,
	max(totals.visits) AS sessions,
	max(totals.pageviews) AS pageviews
	FROM union_session, unnest(hits) h
	WHERE channelgrouping = 'Organic Search'
	AND _TABLE_SUFFIX > FORMAT_DATE("%Y%m%d", DATE(2020,05,01))  
	GROUP BY 
	visitId,
	visitStartTime,
	clientId,
	geoNetwork.country,
	date,
	lower(trim(regexp_replace(regexp_replace(replace(replace(replace(replace(h.appInfo.landingScreenName,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),r'\#.*$',''),'/'))
)
select
    date,
    date_trunc(date, month) month_date,
    landing_page,
    country,
    sum(sessions) sessions,
    sum(pageviews) pageviews,
    count(case when affiliate_clicks_conversion = 1 then clientId end) affiliate_click_conversions,
    count(case when affiliate_top10_clicks_conversion = 1 then clientId end) affiliate_top10_click_conversions,
    count(case when complaint_received_conversion = 1 then clientId end) complaint_received_conversions,
    count(case when affiliate_clicks_conversion = 1 then clientId end) + count(case when affiliate_top10_clicks_conversion = 1 then clientId end) + count(case when complaint_received_conversion = 1 then clientId end) as conversions
from data
group by date, landing_page, country
