{{
    config(
        materialized="table",
        partition_by={"field": "month_date", "data_type": "date"},
        cluster_by=["month_date"],
    )
}}

select
    month_date,
    unix_month_date,
    site,
    site_domain_subfolder,
    country,
    max(preferred_landing_page_url) preferred_landing_page_url,
    query keyword_text,
    string_agg(distinct keyword_intent, ", ") keyword_intent,
    string_agg(distinct keyword_category, ",") keyword_category,
    string_agg(distinct keyword_subcategory, ",") keyword_subcategory,
    string_agg(distinct keyword_brand, ",") keyword_brand,
    string_agg(distinct page_type, ",") page_type,
    max(rank) rank,
    max(competitor_rank) competitor_rank,
    max(competitor_highest_ranking_page) competitor_highest_ranking_page,
    max(competitor_site_domain) competitor_site_domain,
    max(search_volume) search_volume,
    max(avg_cpc) avg_cpc,
    max(search_volume_competition) search_volume_competition,
    max(share_of_voice) share_of_voice,
    max(is_local_result) is_local_result,
    max(is_featured_snippet) is_featured_snippet,
    max(has_site_links) has_site_links,
    max(has_reviews) has_reviews,
    max(has_video) has_video,
    max(page_serp_features) page_serp_features,
    max(tags) tags
from
    (

        select
            month_date,
            unix_date(month_date) unix_month_date,
            b.site,
            a.site_domain_subfolder,
            a.country,
            preferred_landing_page_url,
            a.query,
            competitor_rank,
            competitor_site_domain,
            competitor_highest_ranking_page,
            b.keyword_intent,
            b.keyword_category,
            b.keyword_subcategory,
            b.keyword_brand,
            b.page_type,
            rank,
            search_volume,
            avg_cpc,
            search_volume_competition,
            share_of_voice,
            is_local_result,
            is_featured_snippet,
            has_site_links,
            has_reviews,
            has_video,
            page_serp_features,
            tags
        from {{ ref("accuranker_monthly_agg") }} a
        left join
            {{ ref("accuranker_keyword_mappings_proc") }} b
            on (a.query = b.query and a.site_domain_subfolder = b.site_domain_subfolder)
    )
group by month_date, unix_month_date, site, site_domain_subfolder, query, country
