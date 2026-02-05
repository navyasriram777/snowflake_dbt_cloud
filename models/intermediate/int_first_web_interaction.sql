{{ config(materialized='table') }}

with interactions_ranks as (
    select
        event_id,
        user_id_clean as user_id,
        utm_campaign,
        event_ts,
        row_number() over (
            partition by user_id_clean
            order by event_ts
        ) as row_rank
    from {{ ref('stg_tracking_web_events') }}
)

select
    event_id,
    user_id,
    utm_campaign,
    event_ts
from interactions_ranks
where row_rank = 1
