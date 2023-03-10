SELECT
month_date
,domain_id
,keyword query
,max(preferred_landing_page_url) preferred_landing_page_url
,min(rank) rank 
,min(competitor_rank) competitor_rank
,max(competitor_site_domain) competitor_site_domain
,STRING_AGG(competitor_highest_ranking_page order by search_volume_competition desc limit 1) competitor_highest_ranking_page
,max(search_volume) search_volume
,min(avg_cpc) avg_cpc 
,max(search_volume_competition) search_volume_competition
,min(share_of_voice) share_of_voice
,max(is_local_result) is_local_result
,max(is_featured_snippet) is_featured_snippet
,max(has_site_links) has_site_links
,max(has_reviews) has_reviews
,max(has_video) has_video
,max(page_serp_features) page_serp_features
,max(tags) tags

FROM 
(
	SELECT 
	date_trunc(parse_date("%m/%d/%Y",split(timestamp, " ")[offset(0)]), month) month_date
	,cast(accuranker_name AS INT64) domain_id
	,keyword
	,lower(trim(regexp_replace(regexp_replace(replace(replace(replace(replace(preferred_landing_page_path,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),r'\#.*$',''),'/')) preferred_landing_page_url
	,id
	,rank
	,competitor_rank
	,regexp_extract(competitor_highest_ranking_page,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)') competitor_site_domain
	,lower(trim(regexp_replace(regexp_replace(replace(replace(replace(replace(competitor_highest_ranking_page,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),r'\#.*$',''),'/')) competitor_highest_ranking_page
	,search_volume
	,avg_cpc
	,search_volume_competition
	,share_of_voice
	,is_local_result
	,is_featured_snippet
	,has_site_links
	,has_reviews
	,has_video
	,page_serp_features
	,tags
	,first_value(time_of_entry) over(partition by accuranker_name, keyword, date_trunc(parse_date("%m/%d/%Y",split(timestamp, " ")[offset(0)]), month) order by time_of_entry desc) fv
	,time_of_entry
	FROM `seo-ag.agency_data_pipeline.accuranker_keywords`
	WHERE keyword IS NOT NULL 
)

WHERE time_of_entry = fv
GROUP BY month_date, domain_id, keyword