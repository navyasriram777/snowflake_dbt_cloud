-- models/staging/stg_lottery_games.sql
{{ config
(
    materialized='view'
) 
}}

select
    game_id,
    game_name,
    jackpot_estimate_inminor / 100 as jackpot_usd
from {{ source('raw_lottery', 'games') }}