SELECT 
site_name_country site_country
,site_name_language site
,primary_domain site_domain_subfolder
,SPLIT(primary_domain, '/')[SAFE_ORDINAL(1)] as site_domain
,SPLIT(primary_domain, '/')[SAFE_ORDINAL(2)] as site_subfolder
,site_country country
,parent_site
,status
,notes
FROM `{{target.project}}.agency_data_pipeline.SITES`
WHERE parent_site IS NOT NULL