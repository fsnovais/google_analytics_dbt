WITH distinct_sites AS
(
	SELECT DISTINCT
	site,
	site_domain_subfolder,
	country
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


SELECT *
FROM
(
	SELECT
	month_date,
	unix_date(month_date) unix_month_date,
	coalesce(distinct_sites.site,root_sites.site) site, 
	coalesce(distinct_sites.site_domain_subfolder, root_sites.site_domain) site_domain_subfolder,
	ga.country,
	landing_page landing_page_url,
	sessions,
	pageviews,
	conversions

	FROM
	(
		SELECT
		month_date,
		landing_page,
		SPLIT(landing_page, '/')[SAFE_ORDINAL(1)] site_domain,
		concat(SPLIT(landing_page, '/')[SAFE_ORDINAL(1)], '/', SPLIT(landing_page, '/')[SAFE_ORDINAL(2)]) site_domain_subfolder,
		country,
		coalesce(sum(sessions),0) sessions,
		coalesce(sum(pageviews),0) pageviews,
		coalesce(sum(conversions),0) conversions
		    
		FROM {{ref('ga_360_proc')}}
		WHERE landing_page IS NOT NULL 
		GROUP BY month_date, landing_page, country
	) ga

	LEFT JOIN distinct_sites
	ON ga.site_domain_subfolder = distinct_sites.site_domain_subfolder
	and ga.country = distinct_sites.country
	LEFT JOIN root_sites
	ON ga.site_domain = root_sites.site_domain	
)

WHERE site IS NOT NULL	