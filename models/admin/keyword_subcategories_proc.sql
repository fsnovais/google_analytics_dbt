SELECT
parent_site
,subcategory subcategory_bucket
,rule
,text
,regex subcategory_regex
,row_number() over (partition by parent_site) as rule_order
FROM `seo-ag.agency_data_pipeline.KEYWORD_SUBCATEGORIES`
WHERE parent_site IS NOT NULL AND subcategory IS NOT NULL AND rule IS NOT NULL AND text IS NOT NULL	
