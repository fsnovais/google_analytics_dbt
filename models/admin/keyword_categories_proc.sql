SELECT
parent_site
,category category_bucket
,rule 
,text
,regex category_regex
,row_number() over (partition by parent_site) as rule_order
FROM `{{target.project}}.agency_data_pipeline.KEYWORD_CATEGORIES`
WHERE parent_site IS NOT NULL AND category IS NOT NULL AND rule IS NOT NULL AND text IS NOT NULL
