WITH distinct_sites AS
(
SELECT DISTINCT
site,
country,
site_domain_subfolder
FROM {{ref('sites_proc')}}
),

root_sites AS
(
SELECT DISTINCT 
site_domain,
max(site) site
FROM {{ref('sites_proc')}}
WHERE site_domain = site_domain_subfolder
GROUP BY site_domain
) 


SELECT 
month_date,
coalesce(distinct_sites.site,root_sites.site) site, 
coalesce(distinct_sites.site_domain_subfolder, root_sites.site_domain) site_domain_subfolder ,
landing_page_url,
gsc.country,
query,
sum(clicks) clicks,
sum(impressions) impressions,
IF(sum(impressions) > 0, sum(position*impressions)/sum(impressions), null) avg_position,
IF(sum(impressions) > 0, sum(clicks) / sum(impressions), null) ctr
FROM (

	SELECT
	month_date,
	country,
	clicks,
	impressions,
	ctr,
	position,
	landing_page_domain,
	concat(SPLIT(landing_page_url, '/')[SAFE_ORDINAL(1)], '/', SPLIT(landing_page_url, '/')[SAFE_ORDINAL(2)]) site_domain_subfolder,
	landing_page_url,
	query
	FROM
	(

		SELECT
		CAST(date_month AS Date) month_date,
		country,
		clicks,
		impressions,
		ctr,
		position,
		regexp_extract(landing_page,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)') landing_page_domain,
		lower(trim(regexp_replace(regexp_replace(replace(replace(replace(replace(landing_page,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),r'\#.*$',''),'/')) as landing_page_url,
		regexp_replace(query,r'[^a-zA-Z0-9]',' ') query,
		time_of_entry,
		first_value(time_of_entry) OVER (PARTITION BY CAST(date_month AS Date), query, landing_page, country ORDER BY time_of_entry DESC) lv
		FROM `seo-ag.agency_data_pipeline.gsc_report_sm`
		WHERE date_month NOT IN ('44531','44287','44317','44256')


	--	{% if is_incremental() %}

	    -- recalculate range with updated raw data
	--    	WHERE year_month in ({{ partitions_to_replace | join(',') }})
	        
	--    {% endif %}

	) 

	WHERE lv = time_of_entry    	
 
 ) gsc

LEFT JOIN distinct_sites
ON gsc.site_domain_subfolder = distinct_sites.site_domain_subfolder
AND gsc.country = distinct_sites.country
LEFT JOIN root_sites
ON gsc.landing_page_domain = root_sites.site_domain	
GROUP BY month_date, coalesce(distinct_sites.site,root_sites.site), coalesce(distinct_sites.site_domain_subfolder, root_sites.site_domain), landing_page_url, gsc.country, query