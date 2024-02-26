import os
import time
import subprocess
import glob
import shutil
import pandas as pd
import numpy as np
import json
import re
from google.cloud import bigquery
from datetime import datetime
from pytz import timezone
import requests
from bs4 import BeautifulSoup
# from google_play_scraper import app, reviews, Sort
from app_store_scraper import AppStore
from pyspark.sql.types import *
from commonFunctions import to_gbq
import warnings
warnings.filterwarnings('ignore')
import logging
logging.basicConfig(level=logging.ERROR)

# # Function to convert pandas DataFrame to Spark DataFrame
# def pandas_to_spark(df, spark):
#     return spark.createDataFrame(df)

# # Function to write Spark DataFrame to BigQuery
# def write_spark_to_bigquery(spark_df, table_name, dataset_name, project_id):
#     spark_df.write.format('bigquery') \
#         .option('table', f'{project_id}.{dataset_name}.{table_name}') \
#         .save()

def dataIngestionApple():
    
    folder_path = os.getcwd().replace("\\", "/")
    # Extract Google API from GitHub Secret Variable
    googleAPI_dict = json.loads(os.environ["GOOGLEAPI"])
    with open("googleAPI.json", "w") as f:
        json.dump(googleAPI_dict, f)

    # Hard-coded variables
    appleAppsSample = 999 # 999 = all samples!
    saveReviews = False
    appleReviewCountPerApp = 40 # in batches of 20! Google's app() function pulls latest 40 reviews per app!!
    requests_per_second = None # None = turn off throttling!
    country = 'us'
    language = 'en'
    project_id =  googleAPI_dict["project_id"]
    rawDataset = "practice_project"
    appleScraped_table_name = 'apple_scraped_TEST'
    appleReview_table_name = 'apple_reviews_TEST'
    appleScraped_db_dataSetTableName = f"{rawDataset}.{appleScraped_table_name}"
    appleScraped_db_path = f"{project_id}.{rawDataset}.{appleScraped_table_name}"
    appleReview_db_dataSetTableName = f"{rawDataset}.{appleReview_table_name}"
    appleReview_db_path = f"{project_id}.{rawDataset}.{appleReview_table_name}"
    # dateTime_db_path = f"{project_id}.{rawDataset}.dateTime"
    # dateTime_csv_path = f"{folder_path}/dateTime.csv"
    googleAPI_json_path = f"{folder_path}/googleAPI.json"
    log_file_path = f"{folder_path}/dataSources/appleDataIngestion.log"

    client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = googleAPI_json_path

    # Apple
    ## Clone the repository
    subprocess.run(["git", "clone", "https://github.com/gauthamp10/apple-appstore-apps.git"])
    ## Change directory to the dataset folder
    os.chdir("apple-appstore-apps/dataset")
    ## Extract the tar.lzma file
    subprocess.run(["tar", "-xvf", "appleAppData.json.tar.lzma"])
    ## Read into DataFrame
    apple = pd.read_json("appleAppData.json")

    # Create tables into Google BigQuery
    try:
        job = client.query(f"DELETE FROM {appleScraped_db_path} WHERE TRUE").result()
    except:
        pass
    client.create_table(bigquery.Table(appleScraped_db_path), exists_ok = True)
    try:
        job = client.query(f"DELETE FROM {appleReview_db_path} WHERE TRUE").result()
    except:
        pass
    client.create_table(bigquery.Table(appleReview_db_path), exists_ok = True)

    apple_main = ['name', 'description', 'applicationCategory', 'datePublished',
                 'operatingSystem', 'authorname', 'authorurl', 'ratingValue', 'reviewCount', 'price', 'priceCurrency', 'star_ratings', 'appId']
    
    apple_reviews = ['appId', 'developerResponse', 'date', 'review', 'rating', 'isEdited', 'title', 'userName']

    reviewCountRange = range(0,appleReviewCountPerApp)

    if appleAppsSample != 999:
        apple = apple.sample(appleAppsSample)

    try:
        os.remove(log_file_path)
    except:
        pass
    
    if requests_per_second != None:
        delay_between_requests = 1 / requests_per_second
    else:
        delay_between_requests = None

    # Data Ingestion using BeautifulSoup

    def appWithThrottle(appId, country = 'us', delay_between_requests = None):

        requests.get(f"https://apps.apple.com/{country}/app/{appId}")
        html_page = requests.get(f"https://apps.apple.com/{country}/app/{appId}")
        soup2 = BeautifulSoup(html_page.text, 'html.parser')
        info_boxes = [json.loads(info_box.text.strip()) for info_box in soup2.find_all('script', {'type': 'application/ld+json'})]
        extracted_info = {}

        if info_boxes:
            info_box = info_boxes[0]  # Assuming you want information from the first box
            extracted_info = {
                'name': info_box.get('name', np.NaN),
                'description': info_box.get('description', np.NaN),
                'applicationCategory': info_box.get('applicationCategory', np.NaN),
                'datePublished': info_box.get('datePublished', np.NaN),
                'operatingSystem': info_box.get('operatingSystem', np.NaN),
                'authorname': info_box['author'].get('name', np.NaN) if 'author' in info_box else np.NaN,
                'authorurl': info_box['author'].get('url', np.NaN) if 'author' in info_box else np.NaN,
                'ratingValue': info_box['aggregateRating'].get('ratingValue', np.NaN) if 'aggregateRating' in info_box else np.NaN,
                'reviewCount': info_box['aggregateRating'].get('reviewCount', np.NaN) if 'aggregateRating' in info_box else np.NaN,
                'price': info_box['offers'].get('price', np.NaN) if 'offers' in info_box else np.NaN,
                'priceCurrency': info_box['offers'].get('priceCurrency', np.NaN) if 'offers' in info_box else np.NaN,
                # 'Category': info_box['offers']['category']
            }

        # Add star rating information to the extracted_info dictionary
        soup = BeautifulSoup(html_page.text, 'html.parser')
        width_styles = soup.find_all('div', class_='we-star-bar-graph__bar__foreground-bar')
        percentages = [style['style'].split(': ')[1].rstrip(';') for style in width_styles]
        star_ratings = ['5 Star', '4 Star', '3 Star', '2 Star', '1 Star']
        result = list(zip(star_ratings, percentages))
        extracted_info['star_ratings'] = result

        if delay_between_requests is not None:
            time.sleep(delay_between_requests)

        return extracted_info

    # Data Ingestion using 'app_store_scraper' API for REVIEWS:

    def reviewsWithThrottle(app_id, app_name = 'anything', country = 'us', reviewCount = 100, delay_between_requests = None):
        info = AppStore(country = country, app_name = app_name, app_id = app_id)
        info.review(how_many = reviewCount)
        
        if delay_between_requests != None:
            time.sleep(delay_between_requests)

        return info.reviews
        
    appsChecked = 0
    mainCount = 0
    reviewCount = 0
    for url in apple.iloc[:, 2]:

        # Extract the portion after the last "/"
        last_slash_index = url.rfind("/")
        if last_slash_index != -1:
            app_string = url[last_slash_index + 1:]

            # Define the regex pattern to match the app ID
            pattern = r'id(\d+)'

            # Search for the pattern in the extracted string
            match = re.search(pattern, app_string)

            # Extract the app ID from the matched string
            if match:
                appId = match.group(1)
            else:
                print("App ID not found.")
        else:
            print("No '/' found in the URL.")

        appsChecked += 1
        appReviewCounts = 0

        try:
            app_results = appWithThrottle(
                                    appId = appId,
                                    country=country,
                                    delay_between_requests = delay_between_requests
                                    )
            # row = list(app_results.values())
            # row = [str(value) for value in app_results.values()]
            # row.append(str(appId))
            app_results['appId'] = appId
            if any(value for value in app_results.values()):
                mainCount += 1
            # load_job = to_gbq(pd.DataFrame(data = [row], columns = apple_main), client, appleScraped_db_dataSetTableName, mergeType = 'WRITE_APPEND')
            load_job = to_gbq(pd.DataFrame(app_results), client, appleScraped_db_dataSetTableName, mergeType = 'WRITE_APPEND')
            load_job.result()
            
            if saveReviews == True:
                review = reviewsWithThrottle(
                    app_id = appId,
                    country = country,
                    reviewCount = appleReviewCountPerApp,
                    delay_between_requests = delay_between_requests
                )
                for count in reviewCountRange:
                    try:
                        developer_response = review[count].get('developerResponse')
                        if developer_response is None:
                            review[count]['developerResponse'] = np.NaN
                        row_values = list(review[count].values())
                        row = {'appId': appId}
                        row['developerResponse'] = developer_response
                        row.update(zip(review[count].keys(), row_values))
                        if row:
                            reviewCount += 1
                        load_job = to_gbq(pd.DataFrame(data = [row], columns = apple_reviews), client, appleReview_db_dataSetTableName, mergeType = 'WRITE_APPEND')
                        load_job.result()
                        appReviewCounts += 1
                    except IndexError:
                        continue

            with open(log_file_path, "a") as log_file:
                log_file.write(f"{appId} -> Successfully saved with {appReviewCounts} review(s). Total: {len(mainCount)} app(s) & {len(reviewCount)} review(s) saved.\n")
            print(f'Apple: {len(mainCount)}/{appsChecked} app(s) & {len(reviewCount)} review(s) saved. {appsChecked}/{len(apple)} ({round(appsChecked/len(apple)*100,1)}%) completed.')

        except Exception as e:
            with open(log_file_path, "a") as log_file:
                log_file.write(f"{appId} -> Error occurred: {e}\n")
            print(f"Apple: {e}")

    # # Create 'dateTime' table and push info into DB
    # job = client.query(f"DELETE FROM {dateTime_db_path} WHERE TRUE").result()
    # client.create_table(bigquery.Table(dateTime_db_path), exists_ok = True)
    # current_time = datetime.now(timezone('Asia/Shanghai'))
    # timestamp_string = current_time.isoformat()
    # dt = datetime.strptime(timestamp_string, '%Y-%m-%dT%H:%M:%S.%f%z')
    # date_time_str = dt.strftime('%d-%m-%Y %H:%M:%S')
    # time_zone = dt.strftime('%z')
    # output = f"{date_time_str}; GMT+{time_zone[2]} (SGT)"
    # dateTime_df = pd.DataFrame(data = [output], columns = ['dateTime'])
    # dateTime_df.to_csv(dateTime_csv_path, header = True, index = False)
    # dateTime_job_config = bigquery.LoadJobConfig(
    #     autodetect=True,
    #     skip_leading_rows=1,
    #     source_format=bigquery.SourceFormat.CSV,
    # )
    # dateTime_config = client.dataset(rawDataset).table('dateTime')
    # with open(dateTime_csv_path, 'rb') as f:
    #     dateTime_load_job = client.load_table_from_file(f, dateTime_config, job_config=dateTime_job_config)
    # dateTime_load_job.result()

    ## Remove files and folder
    try:
        # os.remove(dateTime_csv_path)
        os.remove(googleAPI_json_path)
        shutil.rmtree(f"{folder_path}apple-appstore-apps")
    except:
        pass