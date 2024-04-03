CREATE OR REPLACE DATABASE crypto;
CREATE OR REPLACE SCHEMA crypto.raw;
CREATE OR REPLACE SCHEMA crypto.clean;
CREATE OR REPLACE SCHEMA crypto.consumption;

USE SCHEMA crypto.raw;

                            -- NOTIFICATIONS SETUP WITH AZURE

-- Notification integration to get alerts for every new file in Azure Blob Storage
CREATE NOTIFICATION INTEGRATION coin_snowpipe_event
ENABLED = TRUE
TYPE = QUEUE
NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI = <storage_queue_primary_uri>
AZURE_TENANT_ID = <tenant_id>

DESC NOTIFICATION INTEGRATION coin_snowpipe_event;

-- Notification integration for alerting any errors regarding tasks 
CREATE NOTIFICATION INTEGRATION coin_tasks_events
ENABLED = TRUE
TYPE = QUEUE
NOTIFICATION_PROVIDER = AZURE_EVENT_GRID
DIRECTION = OUTBOUND
AZURE_EVENT_GRID_TOPIC_ENDPOINT = <topic_endpoint>
AZURE_TENANT_ID = <tenant_id>

DESC NOTIFICATION INTEGRATION coin_tasks_events;

                            -- STAGE & SNOWPIPE SETUP

-- Raw stage to keep the file downloaded from Azure Blob Storage
CREATE OR REPLACE STAGE crypto.raw.raw_coin_stage
url = <container_url>
credentials = (azure_sas_token= <sas_token>)

-- Listing raw stage items
ls@raw_coin_stage;

-- Snowpipe for automated file ingesting into the raw stage. 
CREATE OR REPLACE pipe "COIN_PIPE"
AUTO_INGEST = TRUE
INTEGRATION = 'coin_snowpipe_event'
AS
COPY INTO raw.staging_table
FROM @raw_coin_stage
FILE_FORMAT = (TYPE = 'JSON') -- Built-in 'JSON' format chosen, since the API response file is a clean and very well                                           formatted file with no blanks and null values.
ON_ERROR = CONTINUE;

-- Pause / unpause the snowpipe
ALTER PIPE COIN_PIPE
SET PIPE_EXECUTION_PAUSED = FALSE;

-- Snowpipe status monitoring
SELECT system$pipe_status ('COIN_PIPE');

-- Integration step for Snowflake & Azure Blob Storage
CREATE STORAGE INTEGRATION my_azure_si
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'AZURE'
ENABLED = TRUE
AZURE_TENANT_ID = <tenant_id>
STORAGE_ALLOWED_LOCATIONS = (<container_url>)
