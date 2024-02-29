import os
import pandas as pd
import json
from google.cloud import bigquery
from datetime import datetime
from pytz import timezone
from commonFunctions import to_gbq
import warnings
warnings.filterwarnings('ignore')

def dateTime():

    folder_path = os.getcwd().replace("\\", "/")
    # Extract Google API from GitHub Secret Variable
    googleAPI_dict = json.loads(os.environ["GOOGLEAPI"])
    with open("googleAPI.json", "w") as f:
        json.dump(googleAPI_dict, f)

    # Hard-coded variables
    project_id =  googleAPI_dict["project_id"]
    dateTimeDataset = "dateTimeData"
    dateTime_db_path = f"{project_id}.{dateTimeDataset}.dateTime"
    dateTime_db_dataSetTableName = f"{dateTimeDataset}.dateTime"
    googleAPI_json_path = f"{folder_path}/googleAPI.json"

    client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)

    # Extract date & time
    current_time = datetime.now(timezone('Asia/Shanghai'))
    date_time_str = current_time.strftime('%d-%m-%Y %H:%M:%S')
    dateTime_df = pd.DataFrame(data=[date_time_str], columns=['dateTime'])
    dateTime_df['dateTime'] = pd.to_datetime(dateTime_df['dateTime'], format='%d-%m-%Y %H:%M:%S')
    
    # Create 'dateTime' table
    # try:
    #     job = client.query(f"DELETE FROM {dateTime_db_path} WHERE TRUE").result()
    # except:
    #     pass
    client.create_table(bigquery.Table(dateTime_db_path), exists_ok = True)

    # Push data into DB table
    dateTime_df = dateTime_df.astype(str)
    load_job = to_gbq(dateTime_df, client, dateTime_db_dataSetTableName)
    load_job.result()

    ## Remove files and folder
    try:
        os.remove(googleAPI_json_path)
    except:
        pass