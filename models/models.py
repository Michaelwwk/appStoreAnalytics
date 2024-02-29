import pandas as pd
import os
import json
from commonFunctions import to_gbq_parquet, read_gbq_spark
from google.cloud import bigquery, storage
from pyspark.sql import SparkSession

# TODO Follow this template when scripting!!
def finalizedMLModels():

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
    cleanDataset = "cleanData" # TODO TO CHANGE FOLDER NAME
    modelDataset = "modelData" # TODO TO CHANGE FOLDER NAME
    modelGoogleScraped_table_name = 'modelGoogleMain' # TODO CHANGE PATH
    googleScraped_db_dataSetTableName = f"{cleanDataset}.{modelGoogleScraped_table_name}"

    client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)

    sparkDf = read_gbq_spark(spark, client, googleAPI_json_path, GBQfolder = 'dateTimeData', GBQtable = 'dateTime')
    print(sparkDf.show())

    # Need to review syntaxes for below portion!!
    """
    # Convert Spark DF to Parquet format
    ## Define the path where you want to save the Parquet file
    parquet_path = "path/to/save/your/parquet/file" # TODO CHANGE PATH

    ## Write the DataFrame to Parquet format
    df_spark.write.parquet(parquet_path)

    # ## Write the DataFrame to Parquet format with additional options
    # df_spark.write.parquet(
    # parquet_path,
    # mode="overwrite",  # Overwrite the existing files
    # compression="snappy",  # Use Snappy compression codec
    # partitionBy="column_name"  # Partition the data by a specific column
    # )

    # Push Parquet to GBQ
    to_gbq_parquet(parquet_path, client, googleScraped_db_dataSetTableName)
    """

    ## Remove files and folder
    try:
        os.remove(googleAPI_json_path)
    except:
        pass

    # Stop Spark session
    spark.stop()