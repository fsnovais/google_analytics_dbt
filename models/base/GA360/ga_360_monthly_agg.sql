WITH distinct_sites AS
(
	SELECT DISTINCT
	site,
	site_domain_subfolder,
    parent_site,
	country
	FROM {{ref('sites_proc')}}
),

root_sites AS
(
	SELECT DISTINCT 
	site_domain,
    parent_site,
	max(site) site
	FROM {{ref('sites_proc')}}
	WHERE site_domain = site_domain_subfolder
	GROUP BY site_domain, parent_site
),
domain_tenants as 
(
    select distinct
        parent_site
        , string_agg(distinct split(site_domain_subfolder, '/')[safe_ordinal(2)], ', ') as available_tenants
    -- TODO: create source reference
    from {{ref('sites_proc')}}
    group by 1
)

SELECT *
FROM
(
	SELECT
    date,
	month_date,
	unix_date(month_date) unix_month_date,
    coalesce(distinct_sites.parent_site,root_sites.parent_site) parent_site,
	coalesce(distinct_sites.site,root_sites.site) site, 
	coalesce(distinct_sites.site_domain_subfolder, root_sites.site_domain) site_domain_subfolder,
    case
            when (
                select count(1) from unnest(split(available_tenants, ', ')) t
                where t = split(lower(regexp_replace(replace(replace(replace(page_location,'www.',''),'http://',''),'https://',''),r'\#.*$','')), '/')[safe_ordinal(2)]
            ) > 0
                then split(lower(regexp_replace(replace(replace(replace(page_location,'www.',''),'http://',''),'https://',''),r'\#.*$','')), '/')[safe_ordinal(3)]
            else split(lower(regexp_replace(replace(replace(replace(page_location,'www.',''),'http://',''),'https://',''),r'\#.*$','')), '/')[safe_ordinal(2)]
        end as site_section,
	ga.country,
	landing_page landing_page_url,
    hostname,
    source,
    medium,
    deviceCategory,
	sessions,
	pageviews,
	conversions

	FROM
	(
		SELECT
        date,
        month_date,
		landing_page,
		SPLIT(landing_page, '/')[SAFE_ORDINAL(1)] site_domain,
		concat(SPLIT(landing_page, '/')[SAFE_ORDINAL(1)], '/', SPLIT(landing_page, '/')[SAFE_ORDINAL(2)]) site_domain_subfolder,
		country,
        hostname,
        page_location,
        source,
        medium,
        deviceCategory,
		coalesce(sum(sessions),0) sessions,
		coalesce(sum(pageviews),0) pageviews,
		coalesce(sum(conversions),0) conversions
		    
		FROM {{ref('ga_360_proc')}}
		WHERE landing_page IS NOT NULL 
		GROUP BY date,month_date, landing_page, country, hostname, page_location, source, medium, deviceCategory
	) ga

	LEFT JOIN distinct_sites 
	ON ga.site_domain_subfolder = distinct_sites.site_domain_subfolder
	and ga.country = distinct_sites.country
	LEFT JOIN root_sites 
	ON ga.site_domain = root_sites.site_domain	
    LEFT JOIN domain_tenants
    ON distinct_sites.parent_site = domain_tenants.parent_site
)

WHERE site IS NOT NULL	