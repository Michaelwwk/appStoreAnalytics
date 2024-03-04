from common import read_gbq, to_gbq
from google.cloud import bigquery
from dataWrangling.dataWrangling import cleanGoogleScraped_table_name

cleanGoogleScraped_table_name = cleanGoogleScraped_table_name

# TODO Follow this template when scripting!!
def finalizedMLModels(spark, project_id, client):

    # Hard-coded variables
    cleanDataset = "cleanData"
    modelDataset = "modelData"
    cleanGoogleScraped_table_name = cleanGoogleScraped_table_name
    modelGoogleScraped_table_name = 'modelGoogleMain' # TODO CHANGE PATH
    modelGgoogleScraped_db_path = f"{project_id}.{modelDataset}.{modelGoogleScraped_table_name}"

    sparkDf = read_gbq(spark, cleanDataset, cleanGoogleScraped_table_name)
    print(sparkDf.show())

    client.create_table(bigquery.Table(modelGgoogleScraped_db_path), exists_ok = True)
    to_gbq(sparkDf, modelDataset, modelGoogleScraped_table_name)