SELECT 
month_date
,site
,site_domain_subfolder
,country
,query
,string_agg(CASE WHEN preferred_landing_page_url IS NOT NULL THEN concat(site_domain_subfolder, ' : ', preferred_landing_page_url) ELSE NULL END, ", ") preferred_landing_page_url
,MIN(rank) rank 
,MIN(competitor_rank) competitor_rank 
,STRING_AGG(competitor_site_domain ORDER BY search_volume_competition DESC LIMIT 1) competitor_site_domain
,STRING_AGG(competitor_highest_ranking_page ORDER BY search_volume_competition DESC LIMIT 1) competitor_highest_ranking_page
,SUM(search_volume) search_volume
,SAFE_DIVIDE(SUM(avg_cpc*search_volume),SUM(search_volume)) avg_cpc 
,SUM(search_volume_competition) search_volume_competition
,MAX(share_of_voice) share_of_voice
,MAX(is_local_result) is_local_result
,MAX(is_featured_snippet) is_featured_snippet
,MAX(has_site_links) has_site_links
,MAX(has_reviews) has_reviews
,MAX(has_video) has_video
,STRING_AGG(concat(site_domain_subfolder, ' : ', page_serp_features), ", ") page_serp_features
,STRING_AGG(distinct tags, ", ") tags

FROM 

(
	SELECT 
	kw.month_date
	,kw.domain_id
	,kw.query
	,kw.preferred_landing_page_url
	,kw.rank 
	,kw.competitor_rank competitor_rank
	,kw.competitor_site_domain
	,kw.competitor_highest_ranking_page
	,kw.search_volume
	,kw.avg_cpc 
	,kw.search_volume_competition
	,kw.share_of_voice
	,kw.is_local_result
	,kw.is_featured_snippet
	,kw.has_site_links
	,kw.has_reviews
	,kw.has_video
	,kw.page_serp_features
	,kw.tags
	,sites.site
	,sites.site_domain_subfolder
	,sites.country
	FROM {{ref('accuranker_keywords_proc')}} kw
	LEFT JOIN {{ref('accuranker_domains_proc')}} dom
	ON kw.domain_id  = dom.id
	LEFT JOIN {{ref('sites_proc')}} sites
	ON dom.display_name = sites.site_country
) data	

GROUP BY 
month_date
,site
,site_domain_subfolder
,country
,query