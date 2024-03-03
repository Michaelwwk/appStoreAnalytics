import time
import os
import json
from dataWrangling.dataWrangling import dataWrangling
from models.models import finalizedMLModels
from dataSources.dateTime import dateTime
from pyspark.sql import SparkSession
from google.cloud import bigquery

# Start Spark session
spark = SparkSession.builder.master("local").appName("appStoreAnalytics").config('spark.ui.port', '4050').getOrCreate()

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

# Stop Spark session
spark.stop()