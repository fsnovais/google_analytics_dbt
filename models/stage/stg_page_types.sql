{{
    config(
        materialized = "table"
    )
}}

with final as (
    select
        site
        , page_type
        , string_agg(distinct regex, '|') as regex

    -- TODO: create source reference
    from `bi-bq-cwv-global.agency_data_pipeline.page_types`
    group by
        site
        , page_type
)

select * from final
