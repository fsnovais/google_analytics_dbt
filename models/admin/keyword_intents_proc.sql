select
    parent_site,
    intent intent_bucket,
    rule,
    text,
    regex intent_regex,
    row_number() over (partition by parent_site) as rule_order
from `seo-ag.agency_data_pipeline.KEYWORD_INTENTS`
where
    parent_site is not null
    and intent is not null
    and rule is not null
    and text is not null
