import sys
import time
import os
import json
from pyspark.sql import SparkSession
from google.cloud import bigquery
from dataSources.deleteRowsAppleGoogle import deleteRowsAppleGoogle
from dataSources.dataIngestionApple import dataIngestionApple
from dataSources.dataIngestionGoogle import dataIngestionGoogle
from dataWrangling.dataWrangling import dataWrangling
from models.models import finalizedMLModels
from dateTime import dateTime
from commonFunctions import read_gbq, to_gbq

# Hard-coded values
appleMaxSlice = 10 # No. of parts to slice Apple df into
googleMaxSlice = 10 # No. of parts to slice Google df into
wranglingMLDateTime_actionNo = 21 # YAML action no. for wrangling, ML, DateTime
TrainTest_actionNo = 22 # YAML action no. for TrainTest
maxNoOfYMLActionNo = 22 # Total no. of YAML files

### Data Ingestion ###

currentAppleSubDf = 1
currentGoogleSubDf = 1

def dataIngestionFunction(appleSlices, currentAppleSubDf, googleSlices, currentGoogleSubDf, deleteRows = False):
    if deleteRows == True:
        deleteRowsAppleGoogle()
    dataIngestionApple(noOfSlices = appleSlices, subDf = currentAppleSubDf)
    dataIngestionGoogle(noOfSlices = googleSlices, subDf = currentGoogleSubDf)

main_Dict = {}

# Define a function to wrap dataIngestionFunction with pre-defined arguments
def create_data_ingestion_function(apple_slices, current_apple_sub_df, google_slices, current_google_sub_df, delete_rows=False):
    return lambda: dataIngestionFunction(apple_slices, current_apple_sub_df, google_slices, current_google_sub_df, delete_rows)

main_dict = {}

for action_no in range(1, appleMaxSlice + googleMaxSlice + 1): # full range of YAML action no.s for data ingestion
    if action_no == 1:
        main_dict[action_no] = create_data_ingestion_function(appleMaxSlice, currentAppleSubDf, 0, 1, delete_rows=True)
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
        # Configurations
        folder_path = os.getcwd().replace("\\", "/")
        googleAPI_dict = json.loads(os.environ["GOOGLEAPI"])
        with open("googleAPI.json", "w") as f:
            json.dump(googleAPI_dict, f)
        project_id =  googleAPI_dict["project_id"]
        googleAPI_json_path = f"{folder_path}/googleAPI.json"
        client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)
        # Run main functions
        dataWrangling(spark, project_id, client, googleAPI_json_path)
        time.sleep(5)
        finalizedMLModels(spark, project_id, client, googleAPI_json_path)
        dateTime(spark, project_id, client, googleAPI_json_path)
        ## Remove files and folder
        try:
            os.remove(googleAPI_json_path)
        except:
            pass
    else:
        # Set folder path
        folder_path = os.path.abspath(os.path.expanduser('~')).replace("\\", "/")
        folder_path = f"{folder_path}/work/appStoreAnalytics/appStoreAnalytics"
        googleAPI_json_path = f"{folder_path}/googleAPI.json"
        # Extract Google API from GitHub Secret Variable
        googleAPI_dict = json.loads(os.environ["GOOGLEAPI"])
        with open(googleAPI_json_path, "w") as f:
            json.dump(googleAPI_dict, f)
        # Hard-coded variables
        project_id =  googleAPI_dict["project_id"]
        rawDataset = "rawData"
        trainTestDataset = "trainTestData"
        AppleScraped_table_name = 'appleMain'
        AppleReview_table_name = 'appleReview'
        GoogleScraped_table_name = 'googleMain'
        GoogleReview_table_name = 'googleReview'
        client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)
        table_names = [AppleScraped_table_name, AppleReview_table_name, GoogleScraped_table_name, GoogleReview_table_name]
        for table_name in table_names:
            trainTest_dataSetTableName = f"{trainTestDataset}.{table_name}"
            trainTest_db_path = f"{project_id}.{trainTest_dataSetTableName}"
            sparkDf = read_gbq(spark, client, googleAPI_json_path, GBQdataset = rawDataset, GBQtable = table_name)
            client.create_table(bigquery.Table(trainTest_db_path), exists_ok = True)
            to_gbq(sparkDf, client, trainTest_dataSetTableName, mergeType ='WRITE_TRUNCATE', sparkdf = True)
        ## Remove files and folder
        try:
            os.remove(googleAPI_json_path)
        except:
            pass
    # Stop Spark session
    spark.stop()

main_Dict[wranglingMLDateTime_actionNo] = wranglingMLDateTime_TrainTest(trainTest = False)
main_Dict[TrainTest_actionNo] = wranglingMLDateTime_TrainTest(trainTest = True)

### Run above functions conditionally depending on which YAML file is calling it ###

for action_inputNo in range(1, maxNoOfYMLActionNo+1):
    if sys.argv[1] == str(action_inputNo):
        print(action_inputNo)
        print(main_Dict[action_inputNo])
        main_Dict[action_inputNo]
    else:
        print("Invalid YAML file specified.")