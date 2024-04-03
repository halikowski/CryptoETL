USE SCHEMA crypto.raw;

-- Table for storing raw json file in the variant column
CREATE OR REPLACE TABLE raw.staging_table (
    RAW_JSON VARIANT
);

CREATE OR REPLACE STREAM raw_json_stream ON TABLE raw.staging_table
COMMENT = 'Stream for recording changes made to the STAGING_TABLE';

-- Optional stream validation
-- SELECT SYSTEM$STREAM_HAS_DATA('raw.raw_json_stream') AS has_data;

-- This table contains all data from the JSON file, without changing column names, for unbroken import.
CREATE OR REPLACE TABLE raw_coin_data (
    id VARCHAR,
    name VARCHAR,
    symbol VARCHAR,
    rank INTEGER,
    total_supply FLOAT,
    max_supply FLOAT,
    beta_value FLOAT,   
    first_data_at TIMESTAMP,
    last_updated TIMESTAMP,
    price FLOAT,
    volume_24h FLOAT,
    volume_24h_change_24h FLOAT,
    market_cap FLOAT,
    market_cap_change_24h FLOAT,
    percent_change_15m FLOAT,
    percent_change_30m FLOAT,
    percent_change_1h FLOAT,
    percent_change_6h FLOAT,
    percent_change_12h FLOAT,
    percent_change_24h FLOAT,
    percent_change_7d FLOAT,
    percent_change_30d FLOAT,
    percent_change_1y FLOAT,
    ath_price FLOAT,
    ath_date TIMESTAMP,
    percent_from_price_ath FLOAT
);


CREATE OR REPLACE TASK load_raw_json_data
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    ERROR_INTEGRATION = 'COIN_TASKS_EVENTS'
    COMMENT = 'Task for inserting raw data from the flattened JSON file into the RAW_COIN_DATA table'
WHEN
    SYSTEM$STREAM_HAS_DATA('raw.raw_json_stream')
AS
    -- Insert data from raw_stage
BEGIN
    INSERT INTO raw.raw_coin_data (
        id,
        name,
        symbol,
        rank,
        total_supply,
        max_supply,
        beta_value,
        first_data_at,
        last_updated,
        price,
        volume_24h,
        volume_24h_change_24h,
        market_cap,
        market_cap_change_24h,
        percent_change_15m,
        percent_change_30m,
        percent_change_1h,
        percent_change_6h,
        percent_change_12h,
        percent_change_24h,
        percent_change_7d,
        percent_change_30d,
        percent_change_1y,
        ath_price,
        ath_date,
        percent_from_price_ath
    )
    SELECT
        f.value:id::STRING AS id,
        f.value:name::STRING AS name,
        f.value:symbol::STRING AS symbol,
        f.value:rank::INTEGER AS rank,
        f.value:total_supply::FLOAT AS total_supply,
        f.value:max_supply::FLOAT AS max_supply,
        f.value:beta_value::FLOAT AS beta_value,
        f.value:first_data_at::TIMESTAMP AS first_data_at,
        f.value:last_updated::TIMESTAMP AS last_updated,
        f.value:quotes.USD.price::FLOAT AS price,
        f.value:quotes.USD.volume_24h::FLOAT AS volume_24h,
        f.value:quotes.USD.volume_24h_change_24h::FLOAT AS volume_24h_change_24h,
        f.value:quotes.USD.market_cap::FLOAT AS market_cap,
        f.value:quotes.USD.market_cap_change_24h::FLOAT AS market_cap_change_24h,
        f.value:quotes.USD.percent_change_15m::FLOAT AS percent_change_15m,
        f.value:quotes.USD.percent_change_30m::FLOAT AS percent_change_30m,
        f.value:quotes.USD.percent_change_1h::FLOAT AS percent_change_1h,
        f.value:quotes.USD.percent_change_6h::FLOAT AS percent_change_6h,
        f.value:quotes.USD.percent_change_12h::FLOAT AS percent_change_12h,
        f.value:quotes.USD.percent_change_24h::FLOAT AS percent_change_24h,
        f.value:quotes.USD.percent_change_7d::FLOAT AS percent_change_7d,
        f.value:quotes.USD.percent_change_30d::FLOAT AS percent_change_30d,
        f.value:quotes.USD.percent_change_1y::FLOAT AS percent_change_1y,
        f.value:quotes.USD.ath_price::FLOAT AS ath_price,
        f.value:quotes.USD.ath_date::TIMESTAMP AS ath_date,
        f.value:quotes.USD.percent_from_price_ath::FLOAT AS percent_from_price_ath
    FROM raw.staging_table,
    LATERAL FLATTEN(input => RAW_JSON) f;
-- Truncating the staging table, so there is only fresh data to further copy at next file update
    TRUNCATE TABLE raw.staging_table;
END;

CREATE OR REPLACE STREAM raw_coin_data_stream ON TABLE raw.raw_coin_data
COMMENT = 'Stream for recording changes made to RAW_COIN_DATA table';

-- Task resuming
ALTER TASK load_raw_json_data RESUME;

-- Both streams validation
SHOW STREAMS;
