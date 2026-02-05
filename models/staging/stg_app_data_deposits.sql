-- models/staging/stg_app_data_deposits.sql
{{ config(
materialized='view'
) }}

select
    deposit_id,
    user_id,
    deposit_amount_inminor / 100.0 as deposit_amount_usd,
    deposit_timestamp::timestamp as deposit_ts
from {{ source('raw_app_data', 'deposits') }}
