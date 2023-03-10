SELECT 
month_date,
unix_month_date,
site,
site_domain_subfolder,
landing_page_url,
country,
first_value(landing_page_url) over (PARTITION BY month_date, keyword_text ORDER BY clicks desc) keyword_top_landing_page,
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
CASE WHEN url_clicks is NULL THEN url_sessions
	 WHEN url_clicks > 0  THEN clicks/url_clicks * url_sessions 
	 ELSE NULL 
	 END AS keyword_attributed_sessions,
CASE WHEN url_clicks IS NULL THEN url_pageviews
	 WHEN url_clicks > 0  THEN clicks/url_clicks * url_pageviews 
	 ELSE NULL 
	 END AS keyword_attributed_pageviews,
CASE WHEN url_clicks IS NULL THEN url_conversions
	 WHEN url_clicks > 0  THEN clicks/url_clicks * url_conversions 
	 ELSE NULL 
	 END AS keyword_attributed_conversions

FROM 
(
	SELECT 
	coalesce(ga360.month_date, gsc.month_date) month_date,
	coalesce(ga360.unix_month_date, gsc.unix_month_date) unix_month_date,
	coalesce(ga360.site, gsc.site) site,
	coalesce(ga360.site_domain_subfolder, gsc.site_domain_subfolder) site_domain_subfolder,
	coalesce(ga360.landing_page_url, gsc.landing_page_url) landing_page_url,
	coalesce(ga360.country, gsc.country) country,
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
	ga360.conversions url_conversions
	FROM {{ref('ga_360_monthly_agg')}} ga360
	FULL OUTER JOIN {{ref('gsc_keyword_mappings')}} gsc
	ON ga360.landing_page_url = gsc.landing_page_url
	AND ga360.country = gsc.country
	AND ga360.month_date = gsc.month_date 	
	WINDOW W1 AS (PARTITION BY gsc.month_date, gsc.landing_page_url, gsc.country)
)



