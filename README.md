# Real-time cryptocurrency ETL workflow
This is a real-time cloud ETL project for cryptocurrency data using CoinPapirka API.
-What are the newest cryptocurrencies released? 
-Which crytocurrencies seem to be bullish in the recent minutes or hours?
-Which of them are facing so called close-calls with ATHs(All Time High values) and which of them have justt hit a new one recently?
*These and many more questions could have been answered using this automated real-time ETL pipeline.

The API is free to use with no key needed - limited for 2000 rows per query, though. 
Obtaining the data is automated with **Airflow**, the DAG runs evey 5 minutes for the fresh data.
This project incorporates **Azure Blob Storage** as a direct ingestion place for response from API and Snowflake SnowPipe for ingestion into **Snowflake** cloud and further manipulations. 
**Azure Storage Queue & Event hubs** were used to set up the **SnowPipe**. 
Additionally, **Azure Event Grid Viewer** is set for obtaining notifications regarding any warnings or errors during snowflake tasks processing.
Finally, a basic data visualization dashboard is created in Snowflake, showing most important data by hand.

**As it seems logical not everybody would like to spend time setting this project up on their computer to see the workflow, i have prepared an easy to follow visual-documentation 
for show-up purposes - check out the 'CryptoProjectOverview.pdf' file.**
[Project Overview file](/CryptoProjectOverview.pdf)

# If You would like to try this project out, here is step by step instruction:
## 1. Airflow installation
   If You don't have airflow set up yet, the 2 configuration files in the 'airflow' folder might be useful. Personally i have used Docker for it, but feel free to do it other way.
   In case You faced any problems, under this link is a detailed installation process:
   https://sleek-data.blogspot.com/2023/09/how-to-install-airflow-on-windows.html

  The dag file 'my_coin_dag.py' is uploaded in the 'airflow' folder.
## 2. Python packages installation
   In order to run the dag successfully, listed packages are necessary. To install all required, use **pip install -r requirements.txt**
   Once the script is ready, you can visit the localhost:8080 address to access Airflow webUI and see your DAG activity.

## 3. Azure setup
   The python script contains environmental variables which are azure storage connection string and container name. The sql files however do not have env variables and require your own azure credentials.
   So if you would like to run the snowpipe, creating your own blob storage container, storage queue and event hub is necessary.

## 4. Snowflake setup
   When it comes to snowflake, except the azure integration details, the worksheets are ready-to-go, so it is enough to run the pipe and resume the tasks.

## 5. Azure Event Grid Viewer
   The event viewer setup is optional, but might be useful for reporting any errors. My personal viewer is linked up below:
   https://snowflaketasksviewer.azurewebsites.net/

## 6. Daa visualization
   Snowflake is obviously not the best tool for data visualization, but because it is my first project using this platform, i wanted to try it out. Feel free to create your own dashboard, or visit the one i have prepared:
   https://app.snowflake.com/xacnxfo/nj90736/#/cryptodash-dbTpohFJa
