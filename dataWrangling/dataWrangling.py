def dataWrangling():

    import pandas as pd
    import os
    import json
    from main21 import spark
    from commonFunctions import to_gbq_parquet
    from google.cloud import bigquery

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
    rawDataset = "rawData" # TODO TO CHANGE FOLDER NAME
    cleanDataset = "cleanData" # TODO TO CHANGE FOLDER NAME
    cleanGoogleScraped_table_name = 'cleanGoogleMain' # TODO CHANGE PATH
    googleScraped_db_dataSetTableName = f"{cleanDataset}.{cleanGoogleScraped_table_name}"

    client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)

    print(f"dataWrangling: {spark}")

    # # Read data from BigQuery into a Pandas DataFrame
    # df = pd.read_gbq("SELECT * FROM google_scraped_test3", project_id=project_id)
    # df = df.head(50)

    # # Save the DataFrame as a CSV file
    # df.to_csv(f"{folder_path}/test.csv", index=False)

    # # Read the CSV file into a Spark DataFrame
    # df_spark = spark.read.csv(f"{folder_path}/test.csv", header=True, inferSchema=True)

    # Need to review syntaxes for below portion!!
    
    # Read data from BigQuery into a Spark DataFrame
    df_spark = spark.read.format("bigquery") \
        .option("table", "big-data-analytics-415801.rawData.googleMain") \
        .load() # TODO TO CHANGE FOLDER NAME

    # Show the DataFrame schema
    df_spark.printSchema()

    # Show the first few rows of the DataFrame
    df_spark.show()

    # Convert Spark DF to Parquet format
    ## Define the path where you want to save the Parquet file
    parquet_path = f"{folder_path}/test_parquet" # TODO CHANGE PATH

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