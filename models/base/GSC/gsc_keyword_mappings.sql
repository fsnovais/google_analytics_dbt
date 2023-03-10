{{ config(
    materialized='table',
    partition_by={
      "field": "month_date",
      "data_type": "date"},
    cluster_by= ["month_date"]
)}}

SELECT
month_date,
unix_month_date,
site,
site_domain_subfolder,
country,
landing_page_url,
query,
-- channel_grouping,
string_agg(distinct keyword_intent, ", ") keyword_intent,
string_agg(distinct keyword_category, ",") keyword_category,
string_agg(distinct keyword_subcategory, ",") keyword_subcategory,
string_agg(distinct keyword_brand, ",") keyword_brand,
string_agg(distinct page_type, ",") page_type,
max(impressions) impressions,
max(clicks) clicks,
min(avg_position) avg_position,
max(ctr) ctr
FROM (

	SELECT
	month_date,
	unix_date(month_date) unix_month_date,
	b.site,
	a.site_domain_subfolder,
	a.country,
	landing_page_url,
	a.query,
--	'Organic Search' as channel_grouping,
	b.keyword_intent,
	b.keyword_category,
	b.keyword_subcategory, 
	b.keyword_brand,
	b.page_type,
	impressions,
	clicks,
	avg_position,
	ctr
	FROM {{ ref('gsc_proc') }} a
	LEFT JOIN {{ ref('gsc_keyword_mappings_proc')}} b
	ON (
		a.site_domain_subfolder = b.site_domain_subfolder
		AND a.query = b.query
		AND a.country = b.country
	)
)
GROUP BY month_date, unix_month_date, site, site_domain_subfolder, landing_page_url, query, country