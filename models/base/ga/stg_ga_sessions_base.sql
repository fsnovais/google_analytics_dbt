
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "session_date_dt",
            "data_type": "date",
        },
    )
}}

with
    source as (
        select
            parse_date('%Y%m%d',date) as session_date_dt,
            visitorid,
            visitnumber,
            visitid,
            visitstarttime,
            totals,
            trafficsource,
            device,
            geonetwork,
            customdimensions,
            hits,
            fullvisitorid,
            userid,
            clientid,
            channelgrouping,
            socialengagementtype
        from {{ source("ga", "sessions") }}
        where cast(_table_suffix as int64) >= 20170801
    ),
    renamed as (
         select 
            session_date_dt,
            visitorid,
            visitnumber,
            visitid,
            visitstarttime,
            totals.visits,
            totals.hits,
            totals.pageviews,
            totals.timeonsite,
            totals.bounces,
            totals.transactions,
            totals.transactionrevenue,
            totals.newvisits,
            totals.screenviews,
            totals.uniquescreenviews,
            totals.timeonscreen,
            totals.totaltransactionrevenue,
            totals.sessionqualitydim,
            trafficsource.referralpath,
            trafficsource.campaign,
            trafficsource.source,
            trafficsource.medium,
            trafficsource.keyword,
            trafficsource.adcontent,
            trafficsource.adwordsclickinfo.campaignid,
            trafficsource.adwordsclickinfo.adgroupid,
            trafficsource.adwordsclickinfo.creativeid,
            trafficsource.adwordsclickinfo.criteriaid,
            trafficsource.adwordsclickinfo.page,
            trafficsource.adwordsclickinfo.slot,
            trafficsource.adwordsclickinfo.criteriaparameters,
            trafficsource.adwordsclickinfo.gclid,
            trafficsource.adwordsclickinfo.customerid,
            trafficsource.adwordsclickinfo.adnetworktype,
            trafficsource.adwordsclickinfo.targetingcriteria.boomUserlistId,
            trafficsource.adwordsclickinfo.isVideoAd,
            trafficsource.istruedirect,
            trafficsource.campaigncode,
            device.browser,
            device.browserversion,
            device.browsersize,
            device.operatingSystem,
            device.operatingSystemVersion,
            device.ismobile,
            device.mobiledevicebranding,
            device.mobiledevicemodel,
            device.mobileinputselector,
            device.mobiledeviceinfo,
            device.mobileDeviceMarketingName,
            device.flashversion,
            device.javaenabled,
            device.language,
            device.screencolors,
            device.screenresolution,
            device.devicecategory,
            geonetwork.continent,
            geonetwork.subcontinent,
            geonetwork.country,
            geonetwork.region,
            geonetwork.metro,
            geonetwork.city,
            geonetwork.cityid,
            geonetwork.networkdomain,
            geonetwork.latitude,
            geonetwork.longitude,
            geonetwork.networklocation,
            hits.hitNumber,
            hits.time,
            hits.hour,
            hits.minute,
            hits.issecure,
            hits.isinteraction,
            hits.isentrance,
            hits.isexit,
            hits.referer,
            hits.page.pagepath,
            hits.page.hostname,
            hits.page.pagetitle,
            hits.page.searchkeyword,
            hits.page.searchcategory,
            hits.page.pagePathLevel1,
            hits.page.pagePathLevel2,
            hits.page.pagePathLevel3,
            hits.page.pagePathLevel4,
            hits.transaction,
            hits.item,
            hits.contentinfo,
            hits.appinfo,
            hits.exceptioninfo,
            hits.eventinfo,
            hits.product,
            hits.promotion,
            hits.promotionactioninfo,
            hits.refund,
            hits.ecommerceaction,
            hits.experiment,
            hits.publisher,
            hits.customvariables,
            hits.customdimensions,
            hits.customMetrics,
            hits.type,
            hits.social,
            hits.latencytracking,
            hits.sourcePropertyInfo,
            hits.contentgroup,
            hits.datasource,
            hits.publisher_infos,
            fullvisitorid,
            userid,
            clientid,
            channelgrouping,
            socialengagementtype
        from source t,
        unnest(t.hits) as hits
    )
select *
from renamed