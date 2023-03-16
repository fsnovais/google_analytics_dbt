SELECT
date,
month_date,
unix_month_date,
site,
site_domain_subfolder,
hostname,
site_section,
country,
source,
medium,
deviceCategory,
keyword_text,
max(keyword_top_landing_page) keyword_top_landing_page,
max(keyword_intent) keyword_intent,
max(keyword_category) keyword_category,
max(keyword_subcategory) keyword_subcategory,
max(keyword_brand) keyword_brand,
max(page_type) page_type,
max(impressions) impressions,
max(clicks) clicks,
max(avg_position) avg_position,
round(max(sessions),0) sessions,
round(max(pageviews),0) pageviews,
round(max(conversions),0) conversions,
max(rank) rank,
max(competitor_rank) competitor_rank,
max(competitor_site_domain) competitor_site_domain,
max(competitor_highest_ranking_page) competitor_highest_ranking_page,
max(search_volume) search_volume,
max(avg_cpc) avg_cpc,
round(max(search_volume_competition),0) search_volume_competition,
max(share_of_voice) share_of_voice,
max(is_local_result) is_local_result,
max(is_featured_snippet) is_featured_snippet,
max(has_site_links) has_site_links,
max(has_reviews) has_reviews,
max(has_video) has_video,
max(page_serp_features) page_serp_features,
max(tags) tags

FROM 
(
	SELECT *
	FROM 
	{{ dbt_utils.union_relations([ref('ga_gsc_organic_join_keyword'), ref('accuranker_keyword_mappings')]) }}
)

GROUP BY 
date,
month_date,
unix_month_date,
site,
site_domain_subfolder,
hostname,
site_section,
country,
source,
medium,
deviceCategory,
keyword_text
