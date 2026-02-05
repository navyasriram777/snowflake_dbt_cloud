{{ config(
    materialized='table'
) }}

WITH base_customers AS (

    SELECT
        user_id,
        signup_ts
    FROM {{ ref('int_signups') }}

),

first_touch AS (

    SELECT
        user_id,
        event_ts as first_touch_ts,
        utm_campaign
    FROM {{ ref('int_first_web_interaction') }}

),

first_deposit AS (

    SELECT
        user_id,
        deposit_ts as first_deposit_ts,
        deposit_amount_usd
    FROM {{ ref('int_first_time_deposit') }}

),

first_ticket AS (

    SELECT
        user_id,
        first_purchase_ts
    FROM {{ ref('int_first_ticket_purchase') }}

),

user_ltv AS(
SELECT
    user_id,
    SUM(deposit_amount_usd) AS ltv
FROM {{ ref('stg_app_data_deposits') }}
GROUP BY user_id
)

SELECT

    c.user_id,
    c.signup_ts,

    ft.first_touch_ts,
    ft.utm_campaign as campaign,

    fd.first_deposit_ts,
    fd.deposit_amount_usd AS first_deposit_amount,

    fp.first_purchase_ts,

    ltv.ltv,

    CASE
        WHEN ltv.ltv > 10000 THEN 'VIP'
        WHEN ltv.ltv > 3000 THEN 'High Value'
        WHEN ltv.ltv > 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS ltv_segment

FROM base_customers c
LEFT JOIN first_touch ft
    ON c.user_id = ft.user_id
LEFT JOIN first_deposit fd
    ON c.user_id = fd.user_id
LEFT JOIN first_ticket fp
    ON c.user_id = fp.user_id
LEFT JOIN user_ltv ltv
    ON c.user_id = ltv.user_id