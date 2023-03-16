select
    parent_site,
    subcategory subcategory_bucket,
    rule,
    text,
    regex subcategory_regex,
    row_number() over (partition by parent_site) as rule_order
from `seo-ag.agency_data_pipeline.KEYWORD_SUBCATEGORIES`
where
    parent_site is not null
    and subcategory is not null
    and rule is not null
    and text is not null
