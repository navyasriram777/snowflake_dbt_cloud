-- models/staging/stg_lottery_ticket_purchases.sql
{{ config(
    materialized='view'
) }}

select
    purchase_id,
    user_id,
    game_id,
    purchase_amount_usd,
    purchase_timestamp::timestamp as purchase_ts
from {{ source('raw_lottery', 'ticket_purchases') }}