-- models/staging/stg_marketing_funnel_spend.sql
{{ config(
    materialized='view'
) }}

select
    date::date as spend_date,
    campaign_name,
    channel,
    spend_inminor / 100.0 as spend_usd
from {{ source('raw_marketing', 'funnel_spend') }}
