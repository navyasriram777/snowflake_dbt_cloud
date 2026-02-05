{{ config(materialized='table') }}

select
    user_id,
    signup_id,
    signup_ts,
    country
from {{ ref('stg_app_data_signups') }}