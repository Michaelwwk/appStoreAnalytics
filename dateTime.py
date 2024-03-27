import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from pytz import timezone
from common import read_gbq, to_gbq
import warnings
warnings.filterwarnings('ignore')

# Hard-coded variables
dateTimeDataset = "dateTimeData"
dateTime_table_name = "dateTime"

def dateTime(spark, project_id, client):

    dateTime_db_path = f"{project_id}.{dateTimeDataset}.{dateTime_table_name}"
    
    try:
        sparkDf = read_gbq(spark, dateTimeDataset, dateTime_table_name)
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
    load_job = to_gbq(dateTime_df, dateTimeDataset, dateTime_table_name, mergeType = 'WRITE_APPEND', sparkdf = False, allDataTypes = False)
    load_job.result()