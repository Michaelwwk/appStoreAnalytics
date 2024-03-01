import pandas as pd
import os
import json
from commonFunctions import read_gbq, to_gbq
from google.cloud import bigquery

# TODO Follow this template when scripting!!
def finalizedMLModels(spark):

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
    cleanGoogleScraped_table_name = 'cleanGoogleMain' # TODO CHANGE PATH
    modelGoogleScraped_table_name = 'modelGoogleMain' # TODO CHANGE PATH
    modelGoogleScraped_db_dataSetTableName = f"{modelDataset}.{modelGoogleScraped_table_name}"
    modelGgoogleScraped_db_path = f"{project_id}.{modelGoogleScraped_db_dataSetTableName}"

    client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)

    sparkDf = read_gbq(spark, client, googleAPI_json_path, GBQfolder = cleanDataset, GBQtable = cleanGoogleScraped_table_name)
    print(sparkDf.show())

    client.create_table(bigquery.Table(modelGgoogleScraped_db_path), exists_ok = True)
    to_gbq(sparkDf, client, modelGoogleScraped_db_dataSetTableName, mergeType ='WRITE_TRUNCATE', sparkdf = True)

    """
    # Convert Spark DF to Parquet format
    ## Define the path where you want to save the Parquet file
    parquet_path = "path/to/save/your/parquet/file"

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
    to_gbq(parquet_path, client, googleScraped_db_dataSetTableName)
    """

    ## Remove files and folder
    try:
        os.remove(googleAPI_json_path)
    except:
        pass