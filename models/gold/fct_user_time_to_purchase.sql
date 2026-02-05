{{ config(materialized='table') }}

SELECT
    ft.utm_campaign AS acquisition_channel,
    COUNT(*) AS total_players,
    ROUND(AVG(DATEDIFF('day', ft.event_ts, fp.first_purchase_ts)), 2) AS avg_days_to_first_purchase
FROM {{ ref('int_first_web_interaction') }} ft
INNER JOIN {{ ref('int_first_ticket_purchase') }} fp
    ON ft.user_id = fp.user_id
WHERE fp.first_purchase_ts > ft.event_ts  -- ensure purchase happens after first interaction
GROUP BY ft.utm_campaign
ORDER BY avg_days_to_first_purchase ASC
