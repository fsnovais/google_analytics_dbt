{{
    config(
        materialized="table",
        partition_by={"field": "month_date", "data_type": "date"},
        cluster_by=["month_date"],
    )
}}

select
    date,
    month_date,
    unix_month_date,
    site,
    site_domain_subfolder,
    page_hostname,
    country,
    site_section,
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
from
    (

        select
            date,
            month_date,
            unix_date(month_date) unix_month_date,
            b.site,
            a.site_domain_subfolder,
            regexp_extract(
                a.site_domain_subfolder,
                '(?:http[s]?://)?(?:www\\.)?(.*?)(?:(?:/|:)(?:.)*|$)'
            ) as page_hostname,
            a.country,
            site_section,
            landing_page_url,
            a.query,
            -- 'Organic Search' as channel_grouping,
            b.keyword_intent,
            b.keyword_category,
            b.keyword_subcategory,
            b.keyword_brand,
            b.page_type,
            impressions,
            clicks,
            avg_position,
            ctr
        from {{ ref("gsc_proc") }} a
        left join
            {{ ref("gsc_keyword_mappings_proc") }} b
            on (
                a.site_domain_subfolder = b.site_domain_subfolder
                and a.query = b.query
                and a.country = b.country
            )
    )
group by
    date,
    month_date,
    unix_month_date,
    site,
    site_domain_subfolder,
    page_hostname,
    landing_page_url,
    query,
    country,
    site_section
