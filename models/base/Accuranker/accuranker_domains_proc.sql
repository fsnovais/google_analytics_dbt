SELECT

id
,display_name
,google_business_name
,group_account_name

FROM 
(
	SELECT 
	id
	,display_name
	,google_business_name
	,group_account_name
	,first_value(time_of_entry) over(partition by id order by time_of_entry desc) fv
	,time_of_entry
	FROM `seo-ag.agency_data_pipeline.accuranker_domains`
)

WHERE time_of_entry = fv