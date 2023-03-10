{% set keywords = ["page_type", "brand", "category", "subcategory", "intent"] %}



with unique_queries as (

	SELECT DISTINCT
	site,
	site_domain_subfolder,
	country,
	query
	FROM {{ ref('gsc_proc')}}
)


SELECT
site_domain_subfolder,
site,
country,
query,
ifnull(string_agg(distinct keyword_intent, ", "), 'Informational') keyword_intent,
ifnull(string_agg(distinct keyword_category, ","), 'Unmapped') keyword_category,
ifnull(string_agg(distinct keyword_subcategory, ","), 'Unmapped') keyword_subcategory,
ifnull(string_agg(distinct keyword_brand, ","), 'Non-brand') keyword_brand,
ifnull(string_agg(distinct keyword_page_type, ","), 'Unmapped') page_type
FROM (

	SELECT
	a.site_domain_subfolder,
	a.site,
	a.country,
	query
	{% for keyword in keywords %}

	,CASE WHEN {{keyword}}_regex is null THEN null
	 WHEN regexp_contains(query, {{keyword}}_regex) THEN {{keyword}}_bucket 
	 ELSE null END as keyword_{{keyword}}

	{%endfor%}
	
	FROM unique_queries a
	LEFT JOIN {{ ref('keyword_mappings')}} b
	ON (
		a.site_domain_subfolder = b.site_domain_subfolder
		AND a.country = b.country
	)
)
GROUP BY site_domain_subfolder, site, country, query