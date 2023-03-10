SELECT
parent_site
,page_type page_type_bucket
,rule
,text_in_path text
,regex page_type_regex
,row_number() over (partition by parent_site) as rule_order
FROM `seo-ag.agency_data_pipeline.PAGE_TYPES`
WHERE parent_site IS NOT NULL AND page_type IS NOT NULL AND rule IS NOT NULL AND text_in_path IS NOT NULL	