-- models/staging/stg_tracking_web_events.sql
{{ config(
    materialized='view'
) }}

select
    event_id,
    coalesce(user_id, anonymous_user_id) as user_id_clean,
    utm_campaign,
    event_timestamp::timestamp as event_ts
from {{ source('raw_tracking', 'web_events') }}
