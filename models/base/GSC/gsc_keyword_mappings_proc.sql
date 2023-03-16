{% set keywords = ["page_type", "brand", "category", "subcategory", "intent"] %}


with
    unique_queries as (

        select distinct site, site_domain_subfolder, country, query
        from {{ ref("gsc_proc") }}
    )

select
    site_domain_subfolder,
    site,
    country,
    query,
    ifnull(string_agg(distinct keyword_intent, ", "), 'Informational') keyword_intent,
    ifnull(string_agg(distinct keyword_category, ","), 'Unmapped') keyword_category,
    ifnull(
        string_agg(distinct keyword_subcategory, ","), 'Unmapped'
    ) keyword_subcategory,
    ifnull(string_agg(distinct keyword_brand, ","), 'Non-brand') keyword_brand,
    ifnull(string_agg(distinct keyword_page_type, ","), 'Unmapped') page_type
from
    (

        select
            a.site_domain_subfolder,
            a.site,
            a.country,
            query
            {% for keyword in keywords %}

            ,
            case
                when {{ keyword }}_regex is null
                then null
                when regexp_contains(query, {{ keyword }}_regex)
                then {{ keyword }}_bucket
                else null
            end as keyword_{{ keyword }}

            {% endfor %}

        from unique_queries a
        left join
            {{ ref("keyword_mappings") }} b
            on (
                a.site_domain_subfolder = b.site_domain_subfolder
                and a.country = b.country
            )
    )
group by site_domain_subfolder, site, country, query
