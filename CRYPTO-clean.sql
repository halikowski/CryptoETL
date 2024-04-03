USE SCHEMA crypto.clean;

-- This table contains cleaned data with few columns removed, few renamed and PK assigned
CREATE OR REPLACE TABLE clean.clean_coin_data (
    ID VARCHAR PRIMARY KEY,
    NAME VARCHAR,
    RANK INTEGER,
    TOTAL_SUPPLY FLOAT,
    MAX_SUPPLY FLOAT,
    BETA_VALUE FLOAT,
    RELEASE_DATE DATE,
    PRICE_USD FLOAT,
    VOLUME_24H FLOAT,
    VOLUME_24H_CHANGE_24H FLOAT,
    MARKET_CAP FLOAT,
    MARKET_CAP_CHANGE_24H FLOAT,
    PERCENT_CHANGE_15M FLOAT,
    PERCENT_CHANGE_30M FLOAT,
    PERCENT_CHANGE_1H FLOAT,
    PERCENT_CHANGE_6H FLOAT,
    PERCENT_CHANGE_12H FLOAT,
    PERCENT_CHANGE_24H FLOAT,
    PERCENT_CHANGE_7D FLOAT,
    PERCENT_CHANGE_30D FLOAT,
    PERCENT_CHANGE_1Y FLOAT,
    ATH_PRICE_USD FLOAT,
    ATH_DATE TIMESTAMP,
    PERCENT_FROM_PRICE_ATH FLOAT
);

CREATE OR REPLACE TASK load_raw_coin_data
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    ERROR_INTEGRATION = 'COIN_TASKS_EVENTS'
    COMMENT = 'Task for loading fresh, raw, structured data from the RAW_COIN_DATA table to the CLEAN_COIN_DATA table. 
               Minor transformations carried out'
WHEN
    SYSTEM$STREAM_HAS_DATA('CRYPTO.RAW.RAW_COIN_DATA_STREAM')
AS
BEGIN
    INSERT INTO crypto.clean.clean_coin_data (
        ID,
        NAME,
        RANK,
        TOTAL_SUPPLY,
        MAX_SUPPLY,
        BETA_VALUE,
        RELEASE_DATE,
        PRICE_USD,
        VOLUME_24H,
        VOLUME_24H_CHANGE_24H,
        MARKET_CAP,
        MARKET_CAP_CHANGE_24H,
        PERCENT_CHANGE_15M,
        PERCENT_CHANGE_30M,
        PERCENT_CHANGE_1H,
        PERCENT_CHANGE_6H,
        PERCENT_CHANGE_12H,
        PERCENT_CHANGE_24H,
        PERCENT_CHANGE_7D,
        PERCENT_CHANGE_30D,
        PERCENT_CHANGE_1Y,
        ATH_PRICE_USD,
        ATH_DATE,
        PERCENT_FROM_PRICE_ATH
    )
    SELECT
        SYMBOL AS ID,
        NAME,
        RANK,
        TOTAL_SUPPLY,
        MAX_SUPPLY,
        BETA_VALUE,
        TO_DATE(FIRST_DATA_AT) AS RELEASE_DATE,
        PRICE::FLOAT AS PRICE_USD,
        VOLUME_24H::FLOAT AS VOLUME_24H,
        VOLUME_24H_CHANGE_24H,
        MARKET_CAP,
        MARKET_CAP_CHANGE_24H,
        PERCENT_CHANGE_15M,
        PERCENT_CHANGE_30M,
        PERCENT_CHANGE_1H,
        PERCENT_CHANGE_6H,
        PERCENT_CHANGE_12H,
        PERCENT_CHANGE_24H,
        PERCENT_CHANGE_7D,
        PERCENT_CHANGE_30D,
        PERCENT_CHANGE_1Y,
        ATH_PRICE::FLOAT AS ATH_PRICE_USD,
        ATH_DATE,
        PERCENT_FROM_PRICE_ATH
    FROM raw.raw_coin_data;
    -- Truncating the table from RAW schema to avoid merging fresh data with old records at next file update
    TRUNCATE TABLE raw.raw_coin_data;
END;

CREATE OR REPLACE STREAM clean_coin_data_stream ON TABLE clean.clean_coin_data
COMMENT = 'Stream for recording changes made to CLEAN_COIN_DATA table';

-- Task resuming
ALTER TASK load_raw_coin_data RESUME;

