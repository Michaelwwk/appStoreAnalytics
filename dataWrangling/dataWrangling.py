import pandas as pd
import os
import json
from common import read_gbq, to_gbq
from google.cloud import bigquery

# TODO Follow this template when scripting!!
def dataWrangling(spark, project_id, client):

    # Hard-coded variables
    rawDataset = "rawData" # TODO TO CHANGE FOLDER NAME
    cleanDataset = "cleanData" # TODO TO CHANGE FOLDER NAME
    GoogleScraped_table_name = 'googleMain' # TODO CHANGE PATH
    cleanGoogleScraped_table_name = 'cleanGoogleMain' # TODO CHANGE PATH
    cleanGoogleScraped_db_dataSetTableName = f"{cleanDataset}.{cleanGoogleScraped_table_name}"
    cleanGoogleScraped_db_path = f"{project_id}.{cleanGoogleScraped_db_dataSetTableName}"

    sparkDf = read_gbq(spark, GBQdataset = rawDataset, GBQtable = GoogleScraped_table_name)
    print(sparkDf.show())

    client.create_table(bigquery.Table(cleanGoogleScraped_db_path), exists_ok = True)
    to_gbq(sparkDf, cleanGoogleScraped_db_dataSetTableName, sparkdf = True)

    # TODO for both Apple & Google Reviews table, need to sort by appId & date asc then keep last row (drop duplicates, subset columns without developers' replies)!