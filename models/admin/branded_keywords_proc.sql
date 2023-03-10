SELECT
parent_site
,'Brand' as brand_bucket
,rule
,text
,regex as brand_regex
,row_number() over (partition by parent_site) as rule_order
FROM `seo-ag.agency_data_pipeline.BRANDED_KEYWORDS`
WHERE parent_site IS NOT NULL AND rule IS NOT NULL AND text IS NOT NULL
