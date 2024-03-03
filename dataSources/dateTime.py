import os
import pandas as pd
import json
from google.cloud import bigquery
from datetime import datetime
from pytz import timezone
from commonFunctions import read_gbq, to_gbq
import warnings
warnings.filterwarnings('ignore')

def dateTime(spark, project_id, client, googleAPI_json_path):

    # Hard-coded variables
    dateTimeDataset = "dateTimeData"
    dateTime_table_name = "dateTime"
    dateTime_db_dataSetTableName = f"{dateTimeDataset}.{dateTime_table_name}"
    dateTime_db_path = f"{project_id}.{dateTime_db_dataSetTableName}"
    
    try:
        sparkDf = read_gbq(spark, client, googleAPI_json_path, GBQdataset = dateTimeDataset, GBQtable = dateTime_table_name)
        rowNo_int = sparkDf.count()+1
    except:
        rowNo_int = 1

    # Extract date & time
    current_time = datetime.now(timezone('Asia/Shanghai'))
    date_time_str = current_time.strftime('%d-%m-%Y %H:%M:%S')
    dateTime_dict = {}
    dateTime_dict['sortingKey'] = rowNo_int
    dateTime_dict['dateAndTime'] = date_time_str
    dateTime_df = pd.DataFrame(dateTime_dict, index=[0])
    
    # Create 'dateTime' table
    # try:
    #     job = client.query(f"DELETE FROM {dateTime_db_path} WHERE TRUE").result()
    # except:
    #     pass
    client.create_table(bigquery.Table(dateTime_db_path), exists_ok = True)

    # Push data into DB table
    load_job = to_gbq(dateTime_df, client, dateTime_db_dataSetTableName)
    load_job.result()

    ## Remove files and folder
    try:
        os.remove(googleAPI_json_path)
    except:
        pass