{{
    config(
        materialized = "table"
    )
}}

with ga4_all_sources as (
    select * from {{ ref('stg_ga4_events_union') }}
)
, dataset_metadata as (
    -- TODO: create source reference
    select * from `bi-bq-cwv-global.agency_data_pipeline.data_feeds`
)
, domain_tenants as (
    select distinct
        site_name
        , string_agg(distinct split(primary_domain, '/')[safe_ordinal(2)], ', ') as available_tenants
    -- TODO: create source reference
    from `bi-bq-cwv-global.agency_data_pipeline.sites`
    group by 1
)
, unnested as (
    select
        _dbt_source_relation as source_system
        , split(trim(split(_dbt_source_relation, '.')[safe_ordinal(2)], '`'), '_')[safe_ordinal(2)] as ga4_dataset_id
        , parse_date('%Y%m%d',event_date) as event_date_dt
        , event_timestamp
        , lower(replace(trim(event_name), " ", "_")) as event_name
        , event_params
        , event_previous_timestamp
        , event_value_in_usd
        , event_bundle_sequence_id
        , event_server_timestamp_offset
        , user_id
        , user_pseudo_id
        , privacy_info.analytics_storage as privacy_info_analytics_storage
        , privacy_info.ads_storage as privacy_info_ads_storage
        , privacy_info.uses_transient_token as privacy_info_uses_transient_token
        , user_properties
        , user_first_touch_timestamp
        , user_ltv.revenue as user_ltv_revenue
        , user_ltv.currency as user_ltv_currency
        , device.category as device_category
        , device.mobile_brand_name as device_mobile_brand_name
        , device.mobile_model_name as device_mobile_model_name
        , device.mobile_marketing_name as device_mobile_marketing_name
        , device.mobile_os_hardware_model as device_mobile_os_hardware_model
        , device.operating_system as device_operating_system
        , device.operating_system_version as device_operating_system_version
        , device.vendor_id as device_vendor_id
        , device.advertising_id as device_advertising_id
        , device.language as device_language
        , device.is_limited_ad_tracking as device_is_limited_ad_tracking
        , device.time_zone_offset_seconds as device_time_zone_offset_seconds
        , device.browser as device_browser
        , device.browser_version as device_browser_version
        , device.web_info.browser as device_web_info_browser
        , device.web_info.browser_version as device_web_info_browser_version
        , device.web_info.hostname as device_web_info_hostname
        , geo.continent as geo_continent
        , geo.country as geo_country
        , geo.region as geo_region
        , geo.city as geo_city
        , geo.sub_continent as geo_sub_continent
        , geo.metro as geo_metro
        , app_info.id as app_info_id
        , app_info.version as app_info_version
        , app_info.install_store as app_info_install_store
        , app_info.firebase_app_id as app_info_firebase_app_id
        , app_info.install_source as app_info_install_source
        , traffic_source.name as traffic_source_name
        , traffic_source.medium as traffic_source_medium
        , traffic_source.source as traffic_source_source
        , stream_id
        , platform
        , ecommerce
        , items
        , (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id
        , (case when (select value.string_value from unnest(event_params) where key = 'session_engaged') = '1' then 1 end) as session_engaged
        , (select value.int_value from unnest(event_params) where key = 'engagement_time_msec') / 1000 as engagement_time_seconds
        , (select value.string_value from unnest(event_params) where key = 'page_location') as page_location
        , (select value.string_value from unnest(event_params) where key = 'page_title') as page_title
        , (select value.string_value from unnest(event_params) where key = 'page_referrer') as page_referrer
        , (select value.string_value from unnest(event_params) where key = 'campaign') as campaign
        , case 
            when event_name = 'page_view' then 1
            else 0
        end as is_page_view
        , case 
            when event_name = 'purchase' then 1
            else 0
        end as is_purchase
        , case 
            when event_name = 'affiliate_link_click' then 1
            else 0
        end as is_affiliate_link_click

    from ga4_all_sources

    qualify row_number() over(
        partition by
            event_date_dt
            , stream_id
            , user_pseudo_id
            , ga_session_id
            , event_name
            , event_timestamp
            , to_json_string(event_params)
    ) = 1
)
, final as (
    select
        dataset_metadata.site_name
        , to_base64(md5(concat(stream_id, user_pseudo_id, cast(ga_session_id as string)))) as session_key
        , to_base64(md5((concat(cast(event_date_dt as string), cast(extract(hour from timestamp_micros(event_timestamp)) as string), page_location)))) as page_key
        , regexp_extract(page_location, '(?:http[s]?://)?(?:www\\.)?(.*?)(?:(?:/|:)(?:.)*|$)') as page_hostname
        , regexp_extract(page_location, '\\?(.+)') as page_query_string
        , regexp_replace(page_location, r'\?.*$', '') as page_no_query_string
        , lower(regexp_replace(replace(replace(replace(page_location,'www.',''),'http://',''),'https://',''),r'\#.*$','')) as page_url_normalized
        , trim(lower(regexp_replace(replace(replace(replace(page_location, 'www.', ''), 'http://', ''), 'https://', ''), r'\#.*$', '')), '/') as page_url_trimmed
        , case
            when (
                select count(1) from unnest(split(available_tenants, ', ')) t
                where t = split(lower(regexp_replace(replace(replace(replace(page_location,'www.',''),'http://',''),'https://',''),r'\#.*$','')), '/')[safe_ordinal(2)]
            ) > 0
                then split(lower(regexp_replace(replace(replace(replace(page_location,'www.',''),'http://',''),'https://',''),r'\#.*$','')), '/')[safe_ordinal(3)]
            else split(lower(regexp_replace(replace(replace(replace(page_location,'www.',''),'http://',''),'https://',''),r'\#.*$','')), '/')[safe_ordinal(2)]
        end as site_section
        , trim(
            concat(
                regexp_extract(page_location, '(?:http[s]?://)?(?:www\\.)?(.*?)(?:(?:/|:)(?:.)*|$)')
                , '/'
                , split(lower(regexp_replace(replace(replace(replace(page_location,'www.',''),'http://',''),'https://',''),r'\#.*$','')), '/')[safe_ordinal(2)]
            ), '/'
        ) as page_url_language
        , max(traffic_source_medium) over (session_window) as medium
        , max(traffic_source_source) over (session_window) as source
        , unnested.*
    from unnested

    left outer join dataset_metadata
        on dataset_metadata.bigquery_name = unnested.ga4_dataset_id
        and dataset_metadata.platform = 'GA4'

    left outer join domain_tenants
        on domain_tenants.site_name = dataset_metadata.site_name

    window session_window as (
        partition by concat(stream_id, user_pseudo_id, cast(ga_session_id as string))
    )

)

select * from final
