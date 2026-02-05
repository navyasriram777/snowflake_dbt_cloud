USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;


CREATE ROLE DBT_ROLE;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DBT_ROLE;
GRANT USAGE ON DATABASE LOTTERY_ANALYTICS TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA LOTTERY_ANALYTICS.DBT_NAVYA TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA LOTTERY_ANALYTICS.DBT_NAVYA TO ROLE DBT_ROLE;
GRANT CREATE TABLE, CREATE VIEW, MODIFY ON SCHEMA LOTTERY_ANALYTICS.DBT_NAVYA TO ROLE DBT_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA LOTTERY_ANALYTICS.DBT_NAVYA TO ROLE DBT_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ACCOUNTADMIN;
GRANT ROLE DBT_ROLE TO USER navyasriram444;


SELECT CURRENT_USER();

SHOW USERS;

DESCRIBE USER navyasriram444;
SHOW GRANTS TO USER navyasriram444;

SELECT 
CURRENT_USER(),
CURRENT_ROLE(),
CURRENT_WAREHOUSE(),
CURRENT_DATABASE(),
CURRENT_SCHEMA();

USE SCHEMA LOTTERY_ANALYTICS.DBT_NAVYA;

CREATE OR REPLACE TABLE raw_app_data.signups (
    user_id STRING(36) NOT NULL,               -- assuming UUIDs or similar
    signup_id STRING(36) NOT NULL,
    signup_timestamp TIMESTAMP_NTZ NOT NULL,
    country STRING(2)                           -- ISO 3166-1 alpha-2 country codes
);
CREATE OR REPLACE TABLE raw_app_data.deposits (
    deposit_id STRING(36) NOT NULL,
    user_id STRING(36) NOT NULL,
    deposit_amount_inminor NUMBER(18,0) NOT NULL,  -- minor units like cents
    deposit_timestamp TIMESTAMP_NTZ NOT NULL
);
CREATE OR REPLACE TABLE raw_lottery.ticket_purchases (
    purchase_id STRING(36) NOT NULL,
    user_id STRING(36) NOT NULL,
    game_id STRING(36) NOT NULL,
    purchase_amount_usd NUMBER(18,2) NOT NULL,   -- USD with 2 decimals
    purchase_timestamp TIMESTAMP_NTZ NOT NULL
);
CREATE OR REPLACE TABLE raw_lottery.games (
    game_id STRING(36) NOT NULL,
    game_name STRING(50) NOT NULL,                -- e.g., 'Eurojackpot'
    jackpot_estimate_inminor NUMBER(20,0)        -- jackpot in minor units (like cents)
);
CREATE OR REPLACE TABLE raw_marketing.funnel_spend (
    date DATE NOT NULL,
    campaign_name STRING(100) NOT NULL,
    channel STRING(50),
    spend_inminor NUMBER(18,0) NOT NULL          -- spend in minor units
);
CREATE OR REPLACE TABLE raw_tracking.web_events (
    event_id STRING(36) NOT NULL,
    user_id STRING(36),                            -- nullable for anonymous users
    anonymous_user_id STRING(36) NOT NULL,
    utm_campaign STRING(100),
    event_timestamp TIMESTAMP_NTZ NOT NULL
);

select * from raw_app_data.signups;
select * from raw_app_data.deposits;

SELECT * FROM raw_lottery.games;
select * from raw_lottery.ticket_purchases;
SELECT * FROM raw_marketing.funnel_spend;
SELECT * FROM raw_tracking.web_events;
