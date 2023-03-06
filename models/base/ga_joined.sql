{{config(materialized = 'table')}}

select 
    * 
    ,'GA' as source
from {{ref('stg_ga_session_base')}}
union all
select 
    *
    ,'GA4' as source
from {{ref('stg_ga4_events_base')}}