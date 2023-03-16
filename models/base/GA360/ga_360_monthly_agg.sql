with
    distinct_sites as (
        select distinct site, site_domain_subfolder, parent_site, country
        from {{ ref("sites_proc") }}
    ),

    root_sites as (
        select distinct site_domain, parent_site, max(site) site
        from {{ ref("sites_proc") }}
        where site_domain = site_domain_subfolder
        group by site_domain, parent_site
    ),
    domain_tenants as (
        select distinct
            parent_site,
            string_agg(
                distinct split(site_domain_subfolder, '/')[safe_ordinal(2)], ', '
            ) as available_tenants
        -- TODO: create source reference
        from {{ ref("sites_proc") }}
        group by 1
    )

select *
from
    (
        select
            date,
            month_date,
            unix_date(month_date) unix_month_date,
            coalesce(distinct_sites.parent_site, root_sites.parent_site) parent_site,
            coalesce(distinct_sites.site, root_sites.site) site,
            coalesce(
                distinct_sites.site_domain_subfolder, root_sites.site_domain
            ) site_domain_subfolder,
            case
                when
                    (
                        select count(1)
                        from unnest(split(available_tenants, ', ')) t
                        where
                            t = split(
                                lower(
                                    regexp_replace(
                                        replace(
                                            replace(
                                                replace(page_location, 'www.', ''),
                                                'http://',
                                                ''
                                            ),
                                            'https://',
                                            ''
                                        ),
                                        r'\#.*$',
                                        ''
                                    )
                                ),
                                '/'
                            )[safe_ordinal(2)]
                    )
                    > 0
                then
                    split(
                        lower(
                            regexp_replace(
                                replace(
                                    replace(
                                        replace(page_location, 'www.', ''),
                                        'http://',
                                        ''
                                    ),
                                    'https://',
                                    ''
                                ),
                                r'\#.*$',
                                ''
                            )
                        ),
                        '/'
                    )[safe_ordinal(3)]
                else
                    split(
                        lower(
                            regexp_replace(
                                replace(
                                    replace(
                                        replace(page_location, 'www.', ''),
                                        'http://',
                                        ''
                                    ),
                                    'https://',
                                    ''
                                ),
                                r'\#.*$',
                                ''
                            )
                        ),
                        '/'
                    )[safe_ordinal(2)]
            end as site_section,
            ga.country,
            landing_page landing_page_url,
            hostname,
            source,
            medium,
            devicecategory,
            sessions,
            pageviews,
            conversions,
            bounces,
            engaged_sessions,
            engagement_time_seconds

        from
            (
                select
                    date,
                    month_date,
                    landing_page,
                    split(landing_page, '/')[safe_ordinal(1)] site_domain,
                    concat(
                        split(landing_page, '/')[safe_ordinal(1)],
                        '/',
                        split(landing_page, '/')[safe_ordinal(2)]
                    ) site_domain_subfolder,
                    country,
                    hostname,
                    page_location,
                    source,
                    medium,
                    devicecategory,
                    coalesce(sum(sessions), 0) sessions,
                    coalesce(sum(pageviews), 0) pageviews,
                    coalesce(sum(conversions), 0) conversions,
                    coalesce(sum(bounces), 0) bounces,
                    coalesce(sum(engaged_sessions), 0) engaged_sessions,
                    coalesce(sum(engagement_time_seconds), 0) engagement_time_seconds
                from {{ ref("ga_360_proc") }}
                where landing_page is not null
                group by
                    date,
                    month_date,
                    landing_page,
                    country,
                    hostname,
                    page_location,
                    source,
                    medium,
                    devicecategory
            ) ga

        left join
            distinct_sites
            on ga.site_domain_subfolder = distinct_sites.site_domain_subfolder
            and ga.country = distinct_sites.country
        left join root_sites on ga.site_domain = root_sites.site_domain
        left join
            domain_tenants on distinct_sites.parent_site = domain_tenants.parent_site
    )

where site is not null
