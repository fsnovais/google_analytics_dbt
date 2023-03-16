with
    union_session as (
        select *, _table_suffix
        from `seo-ag.183853840.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.105263948.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.165478623.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.175962729.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.225371913.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.179442303.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.200066837.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.200066837.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.77278837.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.176228952.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.183852433.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.176238156.ga_sessions_*`
        union all
        select *, _table_suffix
        from `seo-ag.195930178.ga_sessions_*`
    ),
    data as (
        select distinct
            visitid,
            visitstarttime,
            clientid,
            geonetwork.country,
            trafficsource.source,
            trafficsource.medium,
            device.devicecategory,
            parse_date("%Y%m%d", date) date,
            h.page.hostname,
            h.appinfo.landingscreenname page_location,
            lower(
                trim(
                    regexp_replace(
                        regexp_replace(
                            replace(
                                replace(
                                    replace(
                                        replace(
                                            h.appinfo.landingscreenname, 'www.', ''
                                        ),
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
            ) landing_page,
            max(
                case
                    when
                        h.eventinfo.eventcategory = 'Outbound links'
                        and h.eventinfo.eventaction = 'Click'
                    then 1
                    else 0
                end
            ) affiliate_clicks_conversion,
            max(
                case
                    when
                        h.eventinfo.eventcategory = 'Outbound links'
                        and h.eventinfo.eventaction = 'Click Top 10'
                    then 1
                    else 0
                end
            ) affiliate_top10_clicks_conversion,
            max(
                case
                    when
                        h.type = 'PAGE'
                        and h.page.pagepath like '/complaint-received%'
                        and channelgrouping = 'Organic Search'
                    then 1
                    else 0
                end
            ) complaint_received_conversion,
            max(totals.visits) as sessions,
            max(totals.pageviews) as pageviews
        from union_session, unnest(hits) h
        where
            channelgrouping = 'Organic Search'
            and _table_suffix > format_date("%Y%m%d", date(2020, 05, 01))
        group by
            visitid,
            visitstarttime,
            clientid,
            geonetwork.country,
            trafficsource.source,
            trafficsource.medium,
            device.devicecategory,
            date,
            hostname,
            h.appinfo.landingscreenname,
            lower(
                trim(
                    regexp_replace(
                        regexp_replace(
                            replace(
                                replace(
                                    replace(
                                        replace(
                                            h.appinfo.landingscreenname, 'www.', ''
                                        ),
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
            )
    )

select
    date,
    date_trunc(date, month) month_date,
    hostname,
    page_location,
    landing_page,
    country,
    source,
    medium,
    devicecategory,
    sum(sessions) sessions,
    sum(pageviews) pageviews,
    count(
        case when affiliate_clicks_conversion = 1 then clientid end
    ) affiliate_click_conversions,
    count(
        case when affiliate_top10_clicks_conversion = 1 then clientid end
    ) affiliate_top10_click_conversions,
    count(
        case when complaint_received_conversion = 1 then clientid end
    ) complaint_received_conversions,
    count(case when affiliate_clicks_conversion = 1 then clientid end)
    + count(case when affiliate_top10_clicks_conversion = 1 then clientid end)
    + count(
        case when complaint_received_conversion = 1 then clientid end
    ) as conversions
from data
group by
    date, hostname, page_location, landing_page, country, source, medium, devicecategory
