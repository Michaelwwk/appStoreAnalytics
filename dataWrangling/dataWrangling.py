from common import read_gbq, to_gbq
from google.cloud import bigquery
from dataSources.deleteRowsAppleGoogle import rawDataset, googleScraped_table_name

rawDataset = rawDataset

# TODO Follow this template when scripting!!
def dataWrangling(spark, project_id, client):

    # Hard-coded variables
    rawDataset = rawDataset
    cleanDataset = "cleanData"
    GoogleScraped_table_name = googleScraped_table_name
    cleanGoogleScraped_table_name = 'cleanGoogleMain' # TODO CHANGE PATH
    cleanGoogleScraped_db_path = f"{project_id}.{cleanDataset}.{cleanGoogleScraped_table_name}"

    sparkDf = read_gbq(spark, rawDataset, GoogleScraped_table_name)
    print(sparkDf.show())

    client.create_table(bigquery.Table(cleanGoogleScraped_db_path), exists_ok = True)
    to_gbq(sparkDf, cleanDataset, cleanGoogleScraped_table_name)

    """
    TODO For both Apple & Google Reviews table, need to sort by appId, user ID, comment ID, date, etc
         in ascending order then take last row [drop duplicates(subset = 'columns without developers' replies', keep = 'last')]!
    """