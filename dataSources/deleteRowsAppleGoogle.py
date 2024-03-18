# Hard-coded variables (raw data)
rawDataset = "rawData"
appleScraped_table_name = 'appleMain' # TODO CHANGE PATH
googleScraped_table_name = 'googleMain' # TODO CHANGE PATH
appleReview_table_name = 'appleReview' # TODO CHANGE PATH
googleReview_table_name = 'googleReview' # TODO CHANGE PATH

def deleteRowsAppleGoogle(project_id, client):

    appleScraped_db_path = f"{project_id}.{rawDataset}.{appleScraped_table_name}"
    googleScraped_db_path = f"{project_id}.{rawDataset}.{googleScraped_table_name}"

    try:
        client.query(f"DELETE FROM {appleScraped_db_path} WHERE TRUE").result()
        print(f"Data in {appleScraped_db_path} deleted.")
    except:
        pass

    try:
        client.query(f"DELETE FROM {googleScraped_db_path} WHERE TRUE").result()
        print(f"Data in {googleScraped_db_path} deleted.")
    except:
        pass