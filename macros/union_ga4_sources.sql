{% macro union_ga4_sources() %}

{%- set get_ga4_datasets_query %}
    select
        bigquery_name
    from `bi-bq-cwv-global.agency_data_pipeline.data_feeds`
    where platform = 'GA4'
{%- endset %}

{%- set result_query = run_query(get_ga4_datasets_query) %}

{%- if execute %}
    {%- set dataset_list = result_query.columns[0].values() %}

    {#- assume all tables have the same columns #}
    {%- set columns = adapter.get_columns_in_relation(source('ga4_' ~ dataset_list[0], 'events')) %}
{%- else %}
    {%- set dataset_list = [] %}
    {%- set columns = [] %}
{%- endif %}

{%- set ga4_sources = [] %}
{%- for dataset in dataset_list %}
    {% do ga4_sources.append(source('ga4_' ~ dataset, 'events')) %}
{%- endfor %}

{{- dbt_utils.union_relations(
        relations=ga4_sources
    ) }}

{% endmacro %}