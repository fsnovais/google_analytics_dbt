select
    month_date,
    site,
    site_domain_subfolder,
    country,
    query,
    string_agg(
        case
            when preferred_landing_page_url is not null
            then concat(site_domain_subfolder, ' : ', preferred_landing_page_url)
            else null
        end,
        ", "
    ) preferred_landing_page_url,
    min(rank) rank,
    min(competitor_rank) competitor_rank,
    string_agg(
        competitor_site_domain order by search_volume_competition desc limit 1
    ) competitor_site_domain,
    string_agg(
        competitor_highest_ranking_page order by search_volume_competition desc limit 1
    ) competitor_highest_ranking_page,
    sum(search_volume) search_volume,
    safe_divide(sum(avg_cpc * search_volume), sum(search_volume)) avg_cpc,
    sum(search_volume_competition) search_volume_competition,
    max(share_of_voice) share_of_voice,
    max(is_local_result) is_local_result,
    max(is_featured_snippet) is_featured_snippet,
    max(has_site_links) has_site_links,
    max(has_reviews) has_reviews,
    max(has_video) has_video,
    string_agg(
        concat(site_domain_subfolder, ' : ', page_serp_features), ", "
    ) page_serp_features,
    string_agg(distinct tags, ", ") tags

from

    (
        select
            kw.month_date,
            kw.domain_id,
            kw.query,
            kw.preferred_landing_page_url,
            kw.rank,
            kw.competitor_rank competitor_rank,
            kw.competitor_site_domain,
            kw.competitor_highest_ranking_page,
            kw.search_volume,
            kw.avg_cpc,
            kw.search_volume_competition,
            kw.share_of_voice,
            kw.is_local_result,
            kw.is_featured_snippet,
            kw.has_site_links,
            kw.has_reviews,
            kw.has_video,
            kw.page_serp_features,
            kw.tags,
            sites.site,
            sites.site_domain_subfolder,
            sites.country
        from {{ ref("accuranker_keywords_proc") }} kw
        left join {{ ref("accuranker_domains_proc") }} dom on kw.domain_id = dom.id
        left join {{ ref("sites_proc") }} sites on dom.display_name = sites.site_country
    ) data

group by month_date, site, site_domain_subfolder, country, query
