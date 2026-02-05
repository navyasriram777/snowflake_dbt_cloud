-- models/staging/stg_app_data_signups.sql
{{ config(
    materialized='view'
) }}

select
    user_id,
    signup_id,
    signup_timestamp::timestamp as signup_ts,
    country
from {{ source('raw_app_data', 'signups') }}
