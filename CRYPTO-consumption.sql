USE SCHEMA crypto.consumption;

                                -- CREATING FACT & DIMENSION TABLES
                                
-- A consumption-ready fact table with coin data
CREATE OR REPLACE TABLE consumption.fact_coin (
    ID VARCHAR PRIMARY KEY,
    RANK INTEGER,
    PRICE_USD FLOAT
);

-- A consumption-ready DIM table with coin details
CREATE OR REPLACE TABLE consumption.dim_coin_details (
    ID VARCHAR,
    NAME VARCHAR,
    TOTAL_SUPPLY FLOAT,
    MAX_SUPPLY FLOAT,
    BETA_VALUE FLOAT,
    RELEASE_DATE TIMESTAMP,
    ATH_DATE TIMESTAMP
);

-- A consumption-ready dim table with market data
CREATE OR REPLACE TABLE consumption.dim_market (
    ID VARCHAR,
    ATH_PRICE_USD FLOAT,
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
    PERCENT_FROM_PRICE_ATH FLOAT
);

-- Foreign Key assignments
ALTER TABLE consumption.dim_coin_details
ADD FOREIGN KEY (ID) REFERENCES consumption.fact_coin(ID);

ALTER TABLE consumption.dim_market
ADD FOREIGN KEY (ID) REFERENCES consumption.fact_coin(ID);


                                -- CREATING TASKS FOR EVERY TABLE

CREATE OR REPLACE TASK load_clean_coin_data_fact
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    ERROR_INTEGRATION = 'COIN_TASKS_EVENTS'
    COMMENT = 'Task for loading fresh records from the CLEAN_COIN_DATA table into the final fact table FACT_COIN'
WHEN
    SYSTEM$STREAM_HAS_DATA('CRYPTO.CLEAN.CLEAN_COIN_DATA_STREAM')
AS
BEGIN
-- Truncating the table to remove old records before next file update
    TRUNCATE TABLE consumption.fact_coin;
    -- Inserting fresh data
    INSERT INTO consumption.fact_coin(
        ID,
        RANK,
        PRICE_USD
    )
    SELECT
        ID,
        RANK,
        PRICE_USD
    FROM clean.clean_coin_data;
END;


CREATE OR REPLACE TASK load_clean_coin_data_dim_details
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Child task for loading fresh data to the final dimension table DIM_COIN_DETAILS - related to the FACT_COIN table.'
AFTER
    load_clean_coin_data_fact
AS
BEGIN
-- Truncating the table to remove old records before next file update
    TRUNCATE TABLE consumption.dim_coin_details;
    INSERT INTO consumption.dim_coin_details(
        ID,
        NAME,
        TOTAL_SUPPLY,
        MAX_SUPPLY,
        BETA_VALUE,
        RELEASE_DATE,
        ATH_DATE
    )
    SELECT
        ID,
        NAME,
        TOTAL_SUPPLY,
        MAX_SUPPLY,
        BETA_VALUE,
        RELEASE_DATE,
        ATH_DATE
    FROM clean.clean_coin_data;
END;


CREATE OR REPLACE TASK load_clean_coin_data_dim_market
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Child task for loading fresh data into the final dimension table DIM_MARKET - related to the FACT_COIN table.'
AFTER
    load_clean_coin_data_dim_details
AS
BEGIN
-- Truncating the table to remove old records
    TRUNCATE TABLE consumption.dim_market;
    INSERT INTO consumption.dim_market (
        ID,
        ATH_PRICE_USD,
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
        PERCENT_FROM_PRICE_ATH
    )
    SELECT
        ID,
        ATH_PRICE_USD,
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
        PERCENT_FROM_PRICE_ATH
    FROM clean.clean_coin_data;
    -- Truncating the table from CLEAN stage to avoid merging fresh & old records after next file update
    TRUNCATE TABLE clean.clean_coin_data;
END;

-- Streams check-up
SHOW STREAMS;

-- Task resuming (resuming child tasks is optional)
ALTER TASK load_clean_coin_data_fact RESUME;
ALTER TASK load_clean_coin_data_dim_details RESUME;
ALTER TASK load_clean_coin_data_dim_market RESUME;

-- Task history check-up
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME;
