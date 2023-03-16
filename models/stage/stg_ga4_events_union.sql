{{ config(materialized="table") }} {{ union_ga4_sources() }}
