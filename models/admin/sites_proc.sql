select
    site_name_country site_country,
    site_name_language site,
    primary_domain site_domain_subfolder,
    split(primary_domain, '/')[safe_ordinal(1)] as site_domain,
    split(primary_domain, '/')[safe_ordinal(2)] as site_subfolder,
    site_country country,
    parent_site,
    status,
    notes
from `seo-ag.agency_data_pipeline.SITES`
where parent_site is not null
