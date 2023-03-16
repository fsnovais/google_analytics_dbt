select
    parent_site,
    'Brand' as brand_bucket,
    rule,
    text,
    regex as brand_regex,
    row_number() over (partition by parent_site) as rule_order
from `seo-ag.agency_data_pipeline.BRANDED_KEYWORDS`
where parent_site is not null and rule is not null and text is not null
