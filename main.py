import sys
import time
import os
import shutil
from pyspark.sql import SparkSession
from google.cloud import bigquery
import concurrent.futures
from dataSources.deleteRowsAppleGoogle \
import rawDataset, appleScraped_table_name, googleScraped_table_name, appleReview_table_name, googleReview_table_name, deleteRowsAppleGoogle
from dataSources.dataIngestionApple import dataIngestionApple
from dataSources.dataIngestionGoogle import dataIngestionGoogle
from dataWrangling.dataWrangling import dataWrangling
from models.models import finalizedMLModels
from dateTime import dateTime
from common import client, project_id, googleAPI_json_path, folder_path, read_gbq, to_gbq

# Hard-coded variables (impt!)
appleMaxSlice = 26 # No. of parts to slice Apple df into
googleMaxSlice = 14 # No. of parts to slice Google df into
wranglingMLDateTime_actionNo = 41 # YAML action no. for wrangling, ML, dateTime
trainTest_actionNo = 42 # YAML action no. for TrainTest AND total no. of YAML files (trainTest always set as last YAML file!)
trainTestDataset = "trainTestData"

main_dict = {}

### Data Ingestion ###

currentAppleSubDf = 1
currentGoogleSubDf = 1

def dataIngestionFunction(appleSlices, currentAppleSubDf, googleSlices, currentGoogleSubDf, deleteRows = False, project_id = project_id, client = client):
    if deleteRows == True:
        deleteRowsAppleGoogle(project_id = project_id, client = client)
    dataIngestionApple(noOfSlices = appleSlices, subDf = currentAppleSubDf, client = client, project_id = project_id)
    dataIngestionGoogle(noOfSlices = googleSlices, subDf = currentGoogleSubDf, client = client, project_id = project_id)

def create_data_ingestion_function(apple_slices, current_apple_sub_df, google_slices, current_google_sub_df, delete_rows = False, project_id = project_id, client = client):
    return lambda: dataIngestionFunction(apple_slices, current_apple_sub_df, google_slices, current_google_sub_df, delete_rows, project_id, client)

for action_no in range(1, appleMaxSlice + googleMaxSlice + 1): # full range of YAML action no.s for data ingestion
    if action_no == 1:
        main_dict[action_no] = create_data_ingestion_function(appleMaxSlice, currentAppleSubDf, 0, 1, delete_rows = True)
        currentAppleSubDf += 1
    elif action_no in range(2, appleMaxSlice + 1): # range of YAML action no.s for Apple ONLY
        main_dict[action_no] = create_data_ingestion_function(appleMaxSlice, currentAppleSubDf, 0, 1)
        currentAppleSubDf += 1
    elif action_no in range(appleMaxSlice + 1, appleMaxSlice + googleMaxSlice + 1): # range of YAML action no.s for Google ONLY
        main_dict[action_no] = create_data_ingestion_function(0, 1, googleMaxSlice, currentGoogleSubDf)
        currentGoogleSubDf += 1

### wrangling, ML, DateTime, TrainTest ###
        
def wrangling_ml_date_time_train_test(trainTest=False):
    # Start Spark session
    spark = SparkSession.builder.master("local").appName("appStoreAnalytics").config('spark.ui.port', '4050').getOrCreate()

    def data_wrangling(spark, project_id, client):
        dataWrangling(spark, project_id, client)
        print('Data wrangling step completed. Clean tables updated.')

    def finalize_ml_models(spark, project_id, client):
        finalizedMLModels(spark, project_id, client)
        print('ML step completed. Model tables updated.')

    def update_date_time(spark, project_id, client):
        dateTime(spark, project_id, client)
        print('Date & time updated.')

    def process_table(table_name):
        train_test_db_path = f"{project_id}.{trainTestDataset}.{table_name}"
        spark_df = read_gbq(spark, rawDataset, table_name)
        client.create_table(bigquery.Table(train_test_db_path), exists_ok=True)
        to_gbq(spark_df, trainTestDataset, table_name)
        print(f'Train/test data transfer for table {table_name} completed.')

    # Execute main functions sequentially
    if not trainTest:
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [executor.submit(func, spark, project_id, client) for func in [data_wrangling, finalize_ml_models, update_date_time]]
            concurrent.futures.wait(futures)

    # Execute train/test data transfer in parallel
    else:
        table_names = [appleScraped_table_name, appleReview_table_name, googleScraped_table_name, googleReview_table_name]
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [executor.submit(process_table, table_name) for table_name in table_names]
            concurrent.futures.wait(futures)

    # Stop Spark session
    spark.stop()

def create_wrangling_ml_date_time_train_test(trainTest=False):
    return lambda: wrangling_ml_date_time_train_test(trainTest)

main_dict[wranglingMLDateTime_actionNo] = create_wrangling_ml_date_time_train_test(trainTest = False)
main_dict[trainTest_actionNo] = create_wrangling_ml_date_time_train_test(trainTest = True)

### Run above functions conditionally depending on which YAML file is calling it ###
for action_inputNo in range(1, trainTest_actionNo+1):
    if sys.argv[1] == str(action_inputNo):
        main_dict[action_inputNo]()

## Remove files and folder
try:
    os.remove(googleAPI_json_path)
    shutil.rmtree(f"{folder_path}apple-appstore-apps")
except:
    pass