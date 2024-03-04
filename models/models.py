import pandas as pd
import os
import json
from common import read_gbq, to_gbq
from google.cloud import bigquery

# TODO Follow this template when scripting!!
def finalizedMLModels(spark, project_id, client):

    # Hard-coded variables
    cleanDataset = "cleanData" # TODO TO CHANGE FOLDER NAME
    modelDataset = "modelData" # TODO TO CHANGE FOLDER NAME
    cleanGoogleScraped_table_name = 'cleanGoogleMain' # TODO CHANGE PATH
    modelGoogleScraped_table_name = 'modelGoogleMain' # TODO CHANGE PATH
    modelGoogleScraped_db_dataSetTableName = f"{modelDataset}.{modelGoogleScraped_table_name}"
    modelGgoogleScraped_db_path = f"{project_id}.{modelGoogleScraped_db_dataSetTableName}"

    sparkDf = read_gbq(spark, GBQdataset = cleanDataset, GBQtable = cleanGoogleScraped_table_name)
    print(sparkDf.show())

    client.create_table(bigquery.Table(modelGgoogleScraped_db_path), exists_ok = True)
    to_gbq(sparkDf, modelGoogleScraped_db_dataSetTableName, mergeType ='WRITE_TRUNCATE', sparkdf = True)