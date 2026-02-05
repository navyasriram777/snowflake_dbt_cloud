{{ config(materialized='table') }}

with purchase_rank as (
    select
        purchase_id,
        user_id,
        game_id,
        purchase_amount_usd,
        purchase_ts as first_purchase_ts,
        row_number() over (partition by user_id order by purchase_ts) as row_rank
    from {{ ref('stg_lottery_ticket_purchases') }}
),

first_purchase as (
    select *
    from purchase_rank
    where row_rank = 1
),

users_signin as (
    select *
    from {{ ref('stg_app_data_signups') }}
),

first_deposit as (
    select *
    from (
        select
            user_id,
            deposit_id,
            deposit_amount_usd,
            deposit_ts,
            row_number() over (partition by user_id order by deposit_ts) as row_rank
        from {{ ref('stg_app_data_deposits') }}
    ) d
    where row_rank = 1
)

select
    u.user_id,
    d.deposit_id,
    d.deposit_amount_usd,
    d.deposit_ts,
    p.purchase_id,
    p.game_id,
    p.purchase_amount_usd,
    p.first_purchase_ts
from users_signin u
left join first_deposit d on u.user_id = d.user_id
left join first_purchase p on u.user_id = p.user_id
where u.signup_ts < d.deposit_ts
  and d.deposit_ts < p.first_purchase_ts
