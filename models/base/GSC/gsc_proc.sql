with
    distinct_sites as (
        select distinct site, parent_site, country, site_domain_subfolder
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

select
    date,
    month_date,
    coalesce(distinct_sites.parent_site, root_sites.parent_site) parent_site,
    coalesce(distinct_sites.site, root_sites.site) site,
    coalesce(
        distinct_sites.site_domain_subfolder, root_sites.site_domain
    ) site_domain_subfolder,
    landing_page_url,
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
                                        replace(landing_page_url, 'www.', ''),
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
                                replace(landing_page_url, 'www.', ''), 'http://', ''
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
                                replace(landing_page_url, 'www.', ''), 'http://', ''
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
    gsc.country,
    query,
    sum(clicks) clicks,
    sum(impressions) impressions,
    if(
        sum(impressions) > 0, sum(position * impressions) / sum(impressions), null
    ) avg_position,
    if(sum(impressions) > 0, sum(clicks) / sum(impressions), null) ctr
from
    (

        select
            date,
            month_date,
            country,
            clicks,
            impressions,
            ctr,
            position,
            landing_page_domain,
            concat(
                split(landing_page_url, '/')[safe_ordinal(1)],
                '/',
                split(landing_page_url, '/')[safe_ordinal(2)]
            ) site_domain_subfolder,
            landing_page_url,
            query
        from
            (

                select
                    cast(time_of_entry as date) date,
                    cast(date_month as date) month_date,
                    country,
                    clicks,
                    impressions,
                    ctr,
                    position,
                    regexp_extract(
                        landing_page, r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)'
                    ) landing_page_domain,
                    lower(
                        trim(
                            regexp_replace(
                                regexp_replace(
                                    replace(
                                        replace(
                                            replace(
                                                replace(landing_page, 'www.', ''),
                                                'http://',
                                                ''
                                            ),
                                            'https://',
                                            ''
                                        ),
                                        '.html',
                                        ''
                                    ),
                                    r'\?.*$',
                                    ''
                                ),
                                r'\#.*$',
                                ''
                            ),
                            '/'
                        )
                    ) as landing_page_url,
                    regexp_replace(query, r'[^a-zA-Z0-9]', ' ') query,
                    time_of_entry,
                    first_value(time_of_entry) over (
                        partition by
                            cast(date_month as date), query, landing_page, country
                        order by time_of_entry desc
                    ) lv
                from `seo-ag.agency_data_pipeline.gsc_report_sm`
                where date_month not in ('44531', '44287', '44317', '44256')

            -- {% if is_incremental() %}
            -- recalculate range with updated raw data
            -- WHERE year_month in ({{ partitions_to_replace | join(',') }})
            -- {% endif %}
            )

        where lv = time_of_entry

    ) gsc

left join
    distinct_sites
    on gsc.site_domain_subfolder = distinct_sites.site_domain_subfolder
    and gsc.country = distinct_sites.country
left join root_sites on gsc.landing_page_domain = root_sites.site_domain
left join domain_tenants on distinct_sites.parent_site = domain_tenants.parent_site
group by
    date,
    month_date,
    coalesce(distinct_sites.parent_site, root_sites.parent_site),
    coalesce(distinct_sites.site, root_sites.site),
    coalesce(distinct_sites.site_domain_subfolder, root_sites.site_domain),
    landing_page_url,
    site_section,
    gsc.country,
    query
