{{ config(materialized='table') }}

select
    tp.user_id,
    max(
        case when g.jackpot_usd >= 1000000 then 1 else 0 end
    ) as played_high_jackpot
from {{ ref('stg_lottery_ticket_purchases') }} tp
inner join {{ ref('stg_lottery_games') }} g
    on tp.game_id = g.game_id
inner join {{ ref('stg_app_data_signups') }} s
    on tp.user_id = s.user_id
where tp.purchase_ts > s.signup_ts  -- only consider purchases after signup
group by tp.user_id
