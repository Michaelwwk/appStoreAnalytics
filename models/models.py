from commonFunctions import read_gbq, to_gbq
from google.cloud import bigquery

# TODO Follow this template when scripting!!
def finalizedMLModels(spark, project_id, client, googleAPI_json_path):

    # Hard-coded variables
    cleanDataset = "cleanData" # TODO TO CHANGE FOLDER NAME
    modelDataset = "modelData" # TODO TO CHANGE FOLDER NAME
    cleanGoogleScraped_table_name = 'cleanGoogleMain' # TODO CHANGE PATH
    modelGoogleScraped_table_name = 'modelGoogleMain' # TODO CHANGE PATH
    modelGoogleScraped_db_dataSetTableName = f"{modelDataset}.{modelGoogleScraped_table_name}"
    modelGgoogleScraped_db_path = f"{project_id}.{modelGoogleScraped_db_dataSetTableName}"

    sparkDf = read_gbq(spark, client, googleAPI_json_path, GBQdataset = cleanDataset, GBQtable = cleanGoogleScraped_table_name)
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