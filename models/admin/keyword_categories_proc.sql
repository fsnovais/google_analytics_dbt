select
    parent_site,
    category category_bucket,
    rule,
    text,
    regex category_regex,
    row_number() over (partition by parent_site) as rule_order
from `seo-ag.agency_data_pipeline.KEYWORD_CATEGORIES`
where
    parent_site is not null
    and category is not null
    and rule is not null
    and text is not null
