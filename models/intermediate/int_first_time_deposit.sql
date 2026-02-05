{{ config(materialized='table') }}

with users_ranks as (
    select
        d.user_id,
        d.deposit_id,
        d.deposit_amount_usd,
        d.deposit_ts,
        row_number() over (
            partition by d.user_id
            order by d.deposit_ts
        ) as row_rank
    from {{ ref('stg_app_data_deposits') }} d
    inner join {{ ref('stg_app_data_signups') }} s
        on d.user_id = s.user_id
    where d.deposit_ts > s.signup_ts
)

select
    user_id,
    deposit_id,
    deposit_amount_usd,
    deposit_ts
from users_ranks
where row_rank = 1
