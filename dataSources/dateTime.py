import os
import time
import subprocess
import glob
import shutil
import pandas as pd
import json
from google.cloud import bigquery
from datetime import datetime
from pytz import timezone
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
    rawDataset = "practice_project"
    dateTime_db_path = f"{project_id}.{rawDataset}.dateTime"
    dateTime_csv_path = f"{folder_path}/dateTime.csv"
    googleAPI_json_path = f"{folder_path}/googleAPI.json"

    client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)

    # Create 'dateTime' table and push info into DB
    job = client.query(f"DELETE FROM {dateTime_db_path} WHERE TRUE").result()
    client.create_table(bigquery.Table(dateTime_db_path), exists_ok = True)
    current_time = datetime.now(timezone('Asia/Shanghai'))
    timestamp_string = current_time.isoformat()
    dt = datetime.strptime(timestamp_string, '%Y-%m-%dT%H:%M:%S.%f%z')
    date_time_str = dt.strftime('%d-%m-%Y %H:%M:%S')
    time_zone = dt.strftime('%z')
    output = f"{date_time_str}; GMT+{time_zone[2]} (SGT)"
    dateTime_df = pd.DataFrame(data = [output], columns = ['dateTime'])
    dateTime_df.to_csv(dateTime_csv_path, header = True, index = False)
    dateTime_job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )
    dateTime_config = client.dataset(rawDataset).table('dateTime')
    with open(dateTime_csv_path, 'rb') as f:
        dateTime_load_job = client.load_table_from_file(f, dateTime_config, job_config=dateTime_job_config)
    dateTime_load_job.result()

    ## Remove files and folder
    try:
        os.remove(dateTime_csv_path)
        os.remove(googleAPI_json_path)
    except:
        pass