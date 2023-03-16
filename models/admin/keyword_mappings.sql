{% set keyword_tables = [
    ref("page_types_proc"),
    ref("branded_keywords_proc"),
    ref("keyword_categories_proc"),
    ref("keyword_subcategories_proc"),
    ref("keyword_intents_proc"),
] %}


{% set keywords = ["page_type", "brand", "category", "subcategory", "intent"] %}


select
    a.parent_site,
    a.rule_order,
    b.site,
    b.site_domain_subfolder,
    b.country
    {% for keyword in keywords %}

    ,
    max({{ keyword }}_bucket) {{ keyword }}_bucket,
    max({{ keyword }}_regex) {{ keyword }}_regex

    {% endfor %}

from (select * from {{ dbt_utils.union_relations(keyword_tables) }}) a

left join {{ ref("sites_proc") }} b on a.parent_site = b.parent_site
group by a.parent_site, a.rule_order, b.site, b.site_domain_subfolder, b.country
