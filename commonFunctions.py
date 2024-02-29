import pandas as pd
import os
import json
from google.cloud import bigquery, storage
# from pyspark.sql import SparkSession

def split_df(df, noOfSlices = 1, subDf = 1):

    if noOfSlices != 0:
        # Assuming df is your DataFrame
        num_parts = noOfSlices

        # Calculate the number of rows in each part
        num_rows = len(df)
        rows_per_part = num_rows // num_parts

        # Initialize a list to store the sub DataFrames
        sub_dfs = []

        # Split the DataFrame into parts
        for i in range(num_parts):
            start_idx = i * rows_per_part
            end_idx = start_idx + rows_per_part
            if i == num_parts - 1:  # For the last part, include the remaining rows
                end_idx = num_rows
            sub_df = df.iloc[start_idx:end_idx]
            sub_dfs.append(sub_df)

        # Select sub DataFrame
        indexOfSubDf = subDf - 1
        small_df = sub_dfs[indexOfSubDf]
        
    else:
        small_df = pd.DataFrame(columns = df.columns)

    return small_df

def to_gbq(pandasDf, client, dataSet_tableName, mergeType ='WRITE_APPEND'):
    df = pandasDf.copy()
    job_config = bigquery.LoadJobConfig(write_disposition=mergeType)
    load_job = client.load_table_from_dataframe(
        df,
        dataSet_tableName,
        job_config=job_config
    )
    return load_job

def to_gbq_parquet(parquet_file_path, client, dataSet_tableName, mergeType='WRITE_TRUNCATE'):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=mergeType,
    )
    load_job = client.load_table_from_uri(
        parquet_file_path,
        dataSet_tableName,
        job_config=job_config
    )
    return load_job

def read_gbq_spark(spark, client, googleAPI_json_path, GBQfolder, GBQtable):

    # # Start Spark session
    # spark = SparkSession.builder.master("local").appName("readFromGBQTableToSparkDF").config('spark.ui.port', '4050').getOrCreate()

    project_id = "big-data-analytics-415801"
    bucket_name = "nusebac_data_storage"
    dataset_id = GBQfolder
    table_id = GBQtable
    file_name = f"{table_id}.csv"

    # Construct the full table reference path
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    folder_path = os.path.abspath(os.path.expanduser('~')).replace("\\", "/")
    folder_path = f"{folder_path}/work/appStoreAnalytics/appStoreAnalytics"

    # Specify the local path to save the file (optional, default is current directory)
    # googleAPI_json_path = f"{folder_path}/googleAPI.json"
    local_file_path = f"{folder_path}/dataSources/{file_name}"

    # googleAPI_dict = json.loads(os.environ["GOOGLEAPI"])
    # with open(googleAPI_json_path, "w") as f:
    #     json.dump(googleAPI_dict, f)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = googleAPI_json_path

    # client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)

    # Export BigQuery table to GCS
    destination_uri = f'gs://{bucket_name}/{file_name}'
    job_config = bigquery.ExtractJobConfig()
    job = client.extract_table(
    table_ref,
    destination_uri,
    location="US",
    job_config=job_config
    )
    job.result()

    print("success in loading table to bucket!!")

    # Download the file
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download the file to the specified local path
    blob.download_to_filename(local_file_path)

    print("success in dl to local!!")
    print(f"{local_file_path}")

    # Read CSV file into PySpark DataFrame
    sparkDf = spark.read.format('csv') \
                        .option("inferSchema","true") \
                        .option("header","true") \
                        .load(local_file_path)

    # try:
    #     os.remove(local_file_path)
    # except:
    #     pass

    # Stop Spark session
    # spark.stop()

    return sparkDf