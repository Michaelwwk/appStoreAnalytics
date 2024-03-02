import os
import json
from commonFunctions import read_gbq, to_gbq
from google.cloud import bigquery
from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.master("local").appName("appStoreAnalytics").config('spark.ui.port', '4050').getOrCreate()

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