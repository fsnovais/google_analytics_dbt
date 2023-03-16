select
    parent_site,
    page_type page_type_bucket,
    rule,
    text_in_path text,
    regex page_type_regex,
    row_number() over (partition by parent_site) as rule_order
from `seo-ag.agency_data_pipeline.PAGE_TYPES`
where
    parent_site is not null
    and page_type is not null
    and rule is not null
    and text_in_path is not null
