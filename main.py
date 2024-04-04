import sys
import time
import os
import shutil
from pyspark.sql import SparkSession
from google.cloud import bigquery
from dataSources.deleteRowsAppleGoogle \
import rawDataset, appleScraped_table_name, googleScraped_table_name, appleReview_table_name, googleReview_table_name, deleteRowsAppleGoogle
from dataSources.dataIngestionApple import dataIngestionApple
from dataSources.dataIngestionGoogle import dataIngestionGoogle
from dataWrangling.dataWrangling import appleDataWrangling, googleDataWrangling
from models.models import appleClassificationModel, googleClassificationModel, appleRecommenderModel, googleRecommenderModel
from dateTime import dateTime
from common import client, project_id, googleAPI_json_path, folder_path, read_gbq, to_gbq

# Hard-coded variables (impt!)
appleMaxSlice = 26 # No. of parts to slice Apple df into
googleMaxSlice = 14 # No. of parts to slice Google df into
appleWrangling_actionNo = 41
googleWrangling_actionNo = 42
appleClassificationModel_actionNo = 43
googleClassificationModel_actionNo = 44
appleRecommenderModel_actionNo = 45
googleRecommenderModel_actionNo = 46
dateTime_actionNo = 47
dev_rawDataset_actionNo = 48 # YAML action no. for dev_rawData AND total no. of YAML files (dev_rawData always set as last YAML file!)
dev_rawDataset = "dev_rawData"

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

### wrangling, ML, DateTime, devRawData ###
        
def wranglingMLDateTime_devRawData(devRawData = False, appleWrangling = False, googleWrangling = False,
                                   appleClassModel = False, googleClassModel = False,
                                   appleRecModel = False, googleRecModel = False, dateAndTime = False):

    # Start Spark session
    spark = SparkSession.builder.master("local").appName("appStoreAnalytics").config('spark.ui.port', '4050').getOrCreate()
    
    if devRawData == False:
        if appleWrangling == True:
            appleDataWrangling(spark, project_id, client)
            print('Apple Data wrangling step completed. Clean tables updated.')
        if googleWrangling == True:
            googleDataWrangling(spark, project_id, client)
            print('Google Data wrangling step completed. Clean tables updated.')
        if appleClassModel == True:
            time.sleep(30)
            appleClassificationModel(spark, project_id, client)
            print('Apple Classification Model step completed. Apple Classification Model tables updated.')
        if googleClassModel == True:
            time.sleep(30)
            googleClassificationModel(spark, project_id, client)
            print('Google Classification Model step completed. Google Classification Model tables updated.')
        if appleRecModel == True:
            time.sleep(30)
            appleRecommenderModel(spark, project_id, client)
            print('Apple Recommender Model step completed. Apple Recommender Model tables updated.')
        if googleRecModel == True:
            time.sleep(30)
            googleRecommenderModel(spark, project_id, client)
            print('Google Recommender Model step completed. Google Recommender Model tables updated.')
        if dateAndTime == True:
            dateTime(spark, project_id, client)
            print('Date & time updated.')
    else:        
        AppleScraped_table_name = "test1" #appleScraped_table_name
        AppleReview_table_name = "test2" #appleReview_table_name
        GoogleScraped_table_name = "test3" #googleScraped_table_name
        GoogleReview_table_name = "test4" #googleReview_table_name
        table_names = [AppleScraped_table_name, AppleReview_table_name, GoogleScraped_table_name, GoogleReview_table_name]
        for table_name in table_names:
            devRawData_db_path = f"{project_id}.{dev_rawDataset}.{table_name}"
            sparkDf = read_gbq(spark, rawDataset, table_name)
            client.create_table(bigquery.Table(devRawData_db_path), exists_ok = True)
            to_gbq(sparkDf, dev_rawDataset, table_name, allDataTypes = False)
        print('Development raw data transfer step completed. Development raw data tables updated.')
        
    # Stop Spark session
    spark.stop()

def create_wranglingMLDateTime_devRawData(devRawData = False, appleWrangling = False, googleWrangling = False,
                                          appleClassModel = False, googleClassModel = False,
                                          appleRecModel = False, googleRecModel = False, dateAndTime = False):
    return lambda: wranglingMLDateTime_devRawData(devRawData, appleWrangling, googleWrangling,
                                                  appleClassModel, googleClassModel,
                                                  appleRecModel, googleRecModel, dateAndTime)

main_dict[appleWrangling_actionNo] = create_wranglingMLDateTime_devRawData(appleWrangling = True)
main_dict[googleWrangling_actionNo] = create_wranglingMLDateTime_devRawData(googleWrangling = True)
main_dict[appleClassificationModel_actionNo] = create_wranglingMLDateTime_devRawData(appleClassModel = True)
main_dict[googleClassificationModel_actionNo] = create_wranglingMLDateTime_devRawData(googleClassModel = True)
main_dict[appleRecommenderModel_actionNo] = create_wranglingMLDateTime_devRawData(appleRecModel = True)
main_dict[googleRecommenderModel_actionNo] = create_wranglingMLDateTime_devRawData(googleRecModel = True)
main_dict[dateTime_actionNo] = create_wranglingMLDateTime_devRawData(dateAndTime = True)
main_dict[dev_rawDataset_actionNo] = create_wranglingMLDateTime_devRawData(devRawData = True)

### Run above functions conditionally depending on which YAML file is calling it ###
for action_inputNo in range(1, dev_rawDataset_actionNo+1):
    if sys.argv[1] == str(action_inputNo):
        main_dict[action_inputNo]()

## Remove files and folder
try:
    os.remove(googleAPI_json_path)
    shutil.rmtree(f"{folder_path}apple-appstore-apps")
except:
    pass