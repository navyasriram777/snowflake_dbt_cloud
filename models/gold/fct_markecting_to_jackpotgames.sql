{{ config(materialized='table') }}

WITH high_jackpot_players AS (
    SELECT
        hjp.user_id,
        fp.first_purchase_ts AS high_jackpot_purchase_ts
    FROM {{ ref('int_participants_of_high_jackpot') }} hjp
    INNER JOIN {{ ref('int_first_ticket_purchase') }} fp
        ON hjp.user_id = fp.user_id
    WHERE hjp.played_high_jackpot = 1
)

SELECT
    ft.utm_campaign AS acquisition_channel,
    COUNT(DISTINCT ft.user_id) AS total_players,
    COUNT(DISTINCT hjp.user_id) AS high_jackpot_players,
    ROUND(
        COUNT(DISTINCT hjp.user_id) / COUNT(DISTINCT ft.user_id) * 100,
        2
    ) AS pct_high_jackpot
FROM {{ ref('int_first_web_interaction') }} ft
LEFT JOIN high_jackpot_players hjp
    ON ft.user_id = hjp.user_id
    AND hjp.high_jackpot_purchase_ts > ft.event_ts  -- only count purchases after first interaction
GROUP BY ft.utm_campaign
ORDER BY pct_high_jackpot DESC
