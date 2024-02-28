import json
import os
from google.cloud import bigquery

appleScraped_table_name = 'apple_scraped_test9' # TODO CHANGE PATH
googleScraped_table_name = 'google_scraped_test9' # TODO CHANGE PATH
appleReview_table_name = 'apple_reviews_test9' # TODO CHANGE PATH
googleReview_table_name = 'google_reviews_test9' # TODO CHANGE PATH

def deleteRowsAppleGoogle():

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
    rawDataset = "practice_project"
    appleScraped_db_path = f"{project_id}.{rawDataset}.{appleScraped_table_name}"
    googleScraped_db_path = f"{project_id}.{rawDataset}.{googleScraped_table_name}"

    client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)

    try:
        job = client.query(f"DELETE FROM {appleScraped_db_path} WHERE TRUE").result()
    except:
        pass

    try:
        job = client.query(f"DELETE FROM {googleScraped_db_path} WHERE TRUE").result()
    except:
        pass

    ## Remove files and folder
    try:
        os.remove(googleAPI_json_path)
    except:
        pass