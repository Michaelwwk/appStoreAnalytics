import pandas as pd
import os
import json
import shutil
from google.cloud import bigquery, storage

# Configurations
folder_path = os.getcwd().replace("\\", "/")
googleAPI_dict = json.loads(os.environ["GOOGLEAPI"])
googleAPI_json_path = f"{folder_path}/googleAPI.json"
with open(googleAPI_json_path, "w") as f:
    json.dump(googleAPI_dict, f)
project_id =  googleAPI_dict["project_id"]
client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)

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

# def read_gbq(spark, GBQdataset, GBQtable, client=client, googleAPI_json_path=googleAPI_json_path,
#              project_id=project_id, folder_path=folder_path):
    
#     project_id = project_id
#     bucket_name = "nusebac_storage"
#     file_name = f"{GBQtable}.csv"

#     # Construct the full table reference path
#     table_ref = f"{project_id}.{GBQdataset}.{GBQtable}"
#     folder_path = folder_path
#     local_file_path = f"{folder_path}/{file_name}"

#     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = googleAPI_json_path

#     # Export BigQuery table to GCS
#     destination_uri = f'gs://{bucket_name}/{file_name}*'
#     job_config = bigquery.ExtractJobConfig()
#     job = client.extract_table(
#         table_ref,
#         destination_uri,
#         location="US",
#         job_config=job_config
#     )
#     job.result()

#     # Download the files
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     blobs = bucket.list_blobs(prefix=file_name)

#     for blob in blobs:
#         # Download each file
#         blob.download_to_filename(f"{local_file_path}_{blob.name.split('/')[-1]}")

#     # Read CSV files into PySpark DataFrame
#     sparkDf = spark.read.format("csv") \
#         .option("inferSchema", "true") \
#         .option("header", "true") \
#         .option("multiline", "true") \
#         .option("escape", "\"") \
#         .csv(f"{local_file_path}*")  # Use wildcard to read all files

#     return sparkDf

def read_gbq(spark, GBQdataset, GBQtable, client=client, googleAPI_json_path=googleAPI_json_path,
             project_id=project_id, folder_path=folder_path):
    project_id = project_id
    bucket_name = "nusebac_storage"
    file_prefix = f"{GBQtable}.csv"

    # Construct the full table reference path
    table_ref = f"{project_id}.{GBQdataset}.{GBQtable}"
    folder_path = folder_path
    local_file_path = f"{folder_path}/"

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = googleAPI_json_path

    # Export BigQuery table to GCS
    destination_uri = f'gs://{bucket_name}/{file_prefix}*'
    job_config = bigquery.ExtractJobConfig()
    job = client.extract_table(
        table_ref,
        destination_uri,
        location="US",
        job_config=job_config
    )
    job.result()

    # Download the files
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=file_prefix)

    for blob in blobs:
        # Download each file
        file_name = blob.name.split('/')[-1]
        new_file_name = file_name.replace(file_prefix, f"{GBQtable}")
        blob.download_to_filename(f"{local_file_path}/{new_file_name}")

    # Read CSV files into PySpark DataFrame
    sparkDf = spark.read.format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("multiline", "true") \
        .option("escape", "\"") \
        .csv(f"{local_file_path}/{GBQtable}*")  # Use wildcard to read all files

    return sparkDf

# def read_gbq(spark, GBQdataset, GBQtable, client = client,
#              googleAPI_json_path = googleAPI_json_path, project_id = project_id, folder_path = folder_path):

#     project_id = project_id
#     bucket_name = "nusebac_storage"
#     file_name = f"{GBQtable}.csv"

#     # Construct the full table reference path
#     table_ref = f"{project_id}.{GBQdataset}.{GBQtable}"
#     folder_path = folder_path
#     local_file_path = f"{folder_path}/{file_name}"

#     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = googleAPI_json_path

#     # Export BigQuery table to GCS
#     destination_uri = f'gs://{bucket_name}/{file_name}'
#     job_config = bigquery.ExtractJobConfig()
#     job = client.extract_table(
#     table_ref,
#     destination_uri,
#     location="US",
#     job_config=job_config
#     )
#     job.result()

#     # Download the file
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     blob = bucket.blob(file_name)

#     # Download the file to the specified local path
#     blob.download_to_filename(local_file_path)

#     # Read CSV file into PySpark DataFrame    
#     sparkDf = spark.read.format("csv") \
#                         .option("inferSchema", "true") \
#                         .option("header", "true") \
#                         .option("multiline", "true") \
#                         .option("escape", "\"") \
#                         .csv(local_file_path)

#     return sparkDf

def to_gbq(dataframe, GBQdataset, GBQtable, sparkdf = True, client = client,
           folder_path = folder_path, mergeType = 'WRITE_TRUNCATE'): # 'WRITE_APPEND' if want to append values!

    if sparkdf == True:

        folder_path = os.getcwd().replace("\\", "/")
        local_file_path = f"{folder_path}/{GBQdataset}.{GBQtable}.parquet"

        dataframe.write.parquet(local_file_path, mode="overwrite")
        df = pd.read_parquet(local_file_path)

        try:
            shutil.rmtree(local_file_path)
        except:
            pass

    else:
        df = dataframe.copy()

    df = df.astype(str) # all columns will be string
    job_config = bigquery.LoadJobConfig(write_disposition=mergeType)
    load_job = client.load_table_from_dataframe(
        df,
        f"{GBQdataset}.{GBQtable}",
        job_config=job_config
    )

    return load_job

    # # if using parquet to bucket method, add "parquet_file_path = None" into the function's params! Put pandas df chunk under Else statement

            # job_config = bigquery.LoadJobConfig(
            # source_format=bigquery.SourceFormat.PARQUET,
            # write_disposition=mergeType,
            # )

            # load_job = client.load_table_from_uri(
            #     parquet_file_path,
            #     dataSet_tableName,
            #     job_config=job_config
            # )

            # # Convert Spark DF to Parquet format
            # ## Define the path where you want to save the Parquet file
            # parquet_path = "path/to/save/your/parquet/file"

            # ## Write the DataFrame to Parquet format
            # df_spark.write.parquet(parquet_path)

            # # ## Write the DataFrame to Parquet format with additional options
            # # df_spark.write.parquet(
            # # parquet_path,
            # # mode="overwrite",  # Overwrite the existing files
            # # compression="snappy",  # Use Snappy compression codec
            # # partitionBy="column_name"  # Partition the data by a specific column
            # # )

            # # Push Parquet to GBQ
            # to_gbq(parquet_path, client, googleScraped_db_dataSetTableName)

# # Function to convert pandas DataFrame to Spark DataFrame
# def pandas_to_spark(df, spark):
#     return spark.createDataFrame(df)

# # Function to write Spark DataFrame to BigQuery
# def write_spark_to_bigquery(spark_df, table_name, dataset_name, project_id):
#     spark_df.write.format('bigquery') \
#         .option('table', f'{project_id}.{dataset_name}.{table_name}') \
#         .save()