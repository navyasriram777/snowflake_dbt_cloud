{{ config(materialized='table') }}

WITH player_first_deposits AS (
    SELECT
        fd.user_id,
        fd.deposit_amount_usd,
        ft.utm_campaign,
        ft.event_ts AS first_interaction_ts,
        fd.deposit_ts
    FROM {{ ref('int_first_time_deposit') }} fd
    LEFT JOIN {{ ref('int_first_web_interaction') }} ft
        ON fd.user_id = ft.user_id
        AND fd.deposit_ts > ft.event_ts  -- only deposits after first interaction
)

SELECT
    ms.channel,
    ms.campaign_name,
    COUNT(pd.user_id) AS total_players,
    SUM(ms.spend_usd) AS total_spend,
    SUM(pd.deposit_amount_usd) AS total_deposits,
    CASE
        WHEN SUM(ms.spend_usd) IS NULL OR SUM(ms.spend_usd) = 0 THEN SUM(pd.deposit_amount_usd)
        ELSE SUM(pd.deposit_amount_usd) / SUM(ms.spend_usd)
    END AS roas
FROM player_first_deposits pd
LEFT JOIN {{ ref('int_marketing_funnel_spend') }} ms
    ON pd.utm_campaign = ms.campaign_name
GROUP BY ms.channel, ms.campaign_name
ORDER BY roas DESC