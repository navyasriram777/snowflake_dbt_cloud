{{ config(materialized='table') }}

select
    spend_date,
    campaign_name,
    channel,
    spend_usd
from {{ ref('stg_marketing_funnel_spend') }}
