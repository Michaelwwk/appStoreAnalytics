import sys
import time
import os
import shutil
from pyspark.sql import SparkSession
from google.cloud import bigquery
from dataSources.deleteRowsAppleGoogle import deleteRowsAppleGoogle
from dataSources.dataIngestionApple import dataIngestionApple
from dataSources.dataIngestionGoogle import dataIngestionGoogle
from dataWrangling.dataWrangling import dataWrangling
from models.models import finalizedMLModels
from dateTime import dateTime
from common import client, project_id, googleAPI_json_path, folder_path, read_gbq, to_gbq

# Hard-coded values (impt!)
appleMaxSlice = 10 # No. of parts to slice Apple df into
googleMaxSlice = 10 # No. of parts to slice Google df into
wranglingMLDateTime_actionNo = 21 # YAML action no. for wrangling, ML, dateTime
maxNoOfYML_actionNo = 22 # Total no. of YAML files AND YAML action no. for TrainTest (default always the last YAML file)

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
        
def wranglingMLDateTime_TrainTest(trainTest = False):
    # Start Spark session
    spark = SparkSession.builder.master("local").appName("appStoreAnalytics").config('spark.ui.port', '4050').getOrCreate()
    if trainTest == False:
        # Run main functions
        dataWrangling(spark, project_id, client)
        time.sleep(5)
        finalizedMLModels(spark, project_id, client)
        dateTime(spark, project_id, client)
    else:
        # Hard-coded variables
        rawDataset = "rawData"
        trainTestDataset = "trainTestData"
        AppleScraped_table_name = 'appleMain'
        AppleReview_table_name = 'appleReview'
        GoogleScraped_table_name = 'googleMain'
        GoogleReview_table_name = 'googleReview'
        table_names = [AppleScraped_table_name, AppleReview_table_name, GoogleScraped_table_name, GoogleReview_table_name]
        for table_name in table_names:
            trainTest_db_path = f"{project_id}.{trainTestDataset}.{table_name}"
            sparkDf = read_gbq(spark, rawDataset, table_name)
            client.create_table(bigquery.Table(trainTest_db_path), exists_ok = True)
            to_gbq(sparkDf, trainTestDataset, table_name)
    # Stop Spark session
    spark.stop()

def create_wranglingMLDateTime_TrainTest(trainTest = False):
    return lambda: wranglingMLDateTime_TrainTest(trainTest)

main_dict[wranglingMLDateTime_actionNo] = create_wranglingMLDateTime_TrainTest(trainTest = False)
main_dict[maxNoOfYML_actionNo] = create_wranglingMLDateTime_TrainTest(trainTest = True)

### Run above functions conditionally depending on which YAML file is calling it ###
for action_inputNo in range(1, maxNoOfYML_actionNo+1):
    if sys.argv[1] == str(action_inputNo):
        main_dict[action_inputNo]()

## Remove files and folder
try:
    os.remove(googleAPI_json_path)
    shutil.rmtree(f"{folder_path}apple-appstore-apps")
except:
    pass