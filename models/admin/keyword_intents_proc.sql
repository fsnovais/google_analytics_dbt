SELECT
parent_site
,intent intent_bucket
,rule 
,text
,regex intent_regex
,row_number() over (partition by parent_site) as rule_order
FROM `seo-ag.agency_data_pipeline.KEYWORD_INTENTS`
WHERE parent_site IS NOT NULL AND intent IS NOT NULL AND rule IS NOT NULL AND text IS NOT NULL

