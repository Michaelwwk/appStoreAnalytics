import os
import subprocess
import glob
import shutil
import pandas as pd
import json
from google.cloud import bigquery
from pandas_gbq import read_gbq
from datetime import datetime
from pytz import timezone
from google_play_scraper import app, reviews, Sort
import warnings
warnings.filterwarnings('ignore')

def dataIngestion():
    
    folder_path = os.getcwd().replace("\\", "/")
    # Extract Google API from GitHub Secret Variable
    googleAPI_dict = json.loads(os.environ["GOOGLEAPI"])
    with open("googleAPI.json", "w") as f:
        json.dump(googleAPI_dict, f)

    # Hard-coded variables
    googleAppsSample = 50 # 999 = all samples!
    reviewCountPerAppPerScore = 50
    country = 'us'
    language = 'en'
    project_id =  googleAPI_dict["project_id"]
    rawDataset = "practice_project"
    googleScraped_csv_path = f"{folder_path}/google_scraped.csv"
    googleReview_csv_path = f"{folder_path}/google_review.csv"
    googleScraped_db_path = f"{project_id}.{rawDataset}.google_scraped"
    googleReview_db_path = f"{project_id}.{rawDataset}.google_review"
    dateTime_db_path = f"{project_id}.{rawDataset}.dateTime"
    dateTime_csv_path = f"{folder_path}/dateTime.csv"
    googleAPI_json_path = f"{folder_path}/googleAPI.json"
    log_file_path = f"{folder_path}/dataSources/googleDataIngestion.log"

    client = bigquery.Client.from_service_account_json(googleAPI_json_path)

    # Apple
    ## Clone the repository
    subprocess.run(["git", "clone", "https://github.com/gauthamp10/apple-appstore-apps.git"])
    ## Change directory to the dataset folder
    os.chdir("apple-appstore-apps/dataset")
    ## Extract the tar.lzma file
    subprocess.run(["tar", "-xvf", "appleAppData.json.tar.lzma"])
    ## Read into DataFrame
    apple = pd.read_json("appleAppData.json")

    # Google
    ## Clone the repository
    subprocess.run(["git", "clone", "https://github.com/gauthamp10/Google-Playstore-Dataset.git"])
    ## Change directory to the dataset folder
    os.chdir("Google-Playstore-Dataset/dataset")
    ## Extract all .tar.gz files
    for f in os.listdir():
        if f.endswith(".tar.gz"):
            subprocess.run(["tar", "-xvf", f])
    combined_csv = "Google-Playstore-Dataset.csv"
    with open(combined_csv, "wb") as outfile:
        for csvfile in glob.glob("Part?.csv"):
            with open(csvfile, "rb") as infile:
                outfile.write(infile.read())
    ## Read into DataFrame
    google = pd.read_csv("Google-Playstore-Dataset.csv") # low_memory = False

    # Data Ingestion using 'google_play_scraper' API:

    google_main = pd.DataFrame(columns = ['title', 'description', 'descriptionHTML',
                            'summary', 'installs', 'minInstalls', 'realInstalls', 'score', 'ratings', 'reviews', 'histogram', 'price', 'free', 'currency', 'sale',
                            'saleTime', 'originalPrice', 'saleText', 'offersIAP', 'inAppProductPrice', 'developer', 'developerId', 'developerAddress', 'genre',
                            'genreId', 'categories', 'contentRating', 'contentRatingDescription', 'adSupported', 'containsAds', 'released',
                            'lastUpdatedOn', 'updated', 'version', 'appId'])

    google_reviews = pd.DataFrame(columns = ['reviewId', 'userName', 'content', 'score', 'thumbsUpCount',
                                'reviewCreatedVersion', 'at', 'replyContent', 'repliedAt', 'appVersion', 'appId'])

    mainFeaturesToDrop = ['url', 'comments', 'video', 'videoImage', 'screenshots', 'headerImage', 'icon', 'privacyPolicy', 'developerWebsite', 'developerEmail']
    reviewFeaturesToDrop = ['userImage']

    reviewCountRange = range(0,reviewCountPerAppPerScore)

    if googleAppsSample != 999:
        google = google.sample(googleAppsSample)

    try:
        os.remove(log_file_path)
    except:
        pass

    for appId in google.iloc[:, 1]:

        appReviewCounts = 0

        try:
            app_results = app(
            appId,
            lang=language, # defaults to 'en'
            country=country # defaults to 'us'
            )

            for feature in mainFeaturesToDrop:
                app_results.pop(feature, None)
            
            row = [value for value in app_results.values()]
            google_main.loc[len(google_main)] = row

            for score in range(1,6):

                review, continuation_token = reviews(
                    appId,
                    lang=language, # defaults to 'en'
                    country=country, # defaults to 'us'
                    sort=Sort.NEWEST, # defaults to Sort.NEWEST
                    count=reviewCountPerAppPerScore, # defaults to 100
                    filter_score_with=score # defaults to None(means all score)
                )

                for count in reviewCountRange:

                    try:
                        for feature in reviewFeaturesToDrop:
                            review[count].pop(feature, None)
                        row = [value for value in review[count].values()]
                        row.append(appId)
                        google_reviews.loc[len(google_reviews)] = row
                        appReviewCounts += 1
                    except IndexError:
                        continue
            
            with open(log_file_path, "a") as log_file:
                log_file.write(f"{appId} -> Successfully saved with {appReviewCounts} review(s). Total: {len(google_main)} app(s) & {len(google_reviews)} review(s) saved.\n")
            
        except Exception as e:
            with open(log_file_path, "a") as log_file:
                log_file.write(f"{appId} .. Error occurred: {e}\n")

    google_main = google_main.astype(str) # all columns will be string!!
    google_main.to_csv(googleScraped_csv_path, header = True, index = False)
    google_reviews.to_csv(googleReview_csv_path, header = True, index = False)

    # Create tables into Google BigQuery
    try:
        job = client.query(f"DELETE FROM {googleScraped_db_path} WHERE TRUE").result()
    except:
        pass
    client.create_table(bigquery.Table(googleScraped_db_path), exists_ok = True)
    try:
        job = client.query(f"DELETE FROM {googleReview_db_path} WHERE TRUE").result()
    except:
        pass
    client.create_table(bigquery.Table(googleReview_db_path), exists_ok = True)

    # Push data into DB
    # google_main = google_main.astype(str) # all columns will be string!!
    # google_main.to_gbq(destination_table=googleScraped_db_path, project_id=project_id, if_exists='replace')
    # google_reviews.to_gbq(destination_table=googleReview_db_path, project_id=project_id, if_exists='replace')

    # Read CSV files into strings
    with open(googleScraped_csv_path, 'r') as f:
        googleScraped_csv_content = f.read()

    with open(googleReview_csv_path, 'r') as f:
        googleReview_csv_content = f.read()

    # Load data into BigQuery tables
    google_scraped_config = client.dataset(rawDataset).table('google_scraped')
    googleReview_config = client.dataset(rawDataset).table('google_review')

    # Push data to BigQuery
    google_scraped_job = client.load_table_from_file(googleScraped_csv_content, google_scraped_config, job_config=googleScraped_db_path)
    googleReview_job = client.load_table_from_file(googleReview_csv_content, googleReview_config, job_config=googleReview_db_path)

    # Wait for job completion
    google_scraped_job.result()
    googleReview_job.result()

    # googleScraped_job_config = bigquery.LoadJobConfig(
    # autodetect=False,
    # max_bad_records=5,
    # skip_leading_rows=1,
    # source_format=bigquery.SourceFormat.CSV
    # )
    # google_scraped_config = client.dataset(rawDataset).table('google_scraped')
    # with open(googleScraped_csv_path, 'rb') as f:
    #     googleScraped_load_job = client.load_table_from_file(f, google_scraped_config, job_config=googleScraped_job_config)
    # googleScraped_load_job.result()

    # googleReview_job_config = bigquery.LoadJobConfig(
    # autodetect=False,
    # max_bad_records=5,
    # skip_leading_rows=1,
    # source_format=bigquery.SourceFormat.CSV
    # )
    # google_review_config = client.dataset(rawDataset).table('google_review')
    # with open(googleReview_csv_path, 'rb') as f:
    #     googleReview_load_job = client.load_table_from_file(f, google_review_config, job_config=googleReview_job_config)
    # googleReview_load_job.result()

    # Create 'dateTime' table in DB
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
        os.remove(googleScraped_csv_path)
        os.remove(googleReview_csv_path)
        os.remove(googleAPI_json_path)
        shutil.rmtree(f"{folder_path}apple-appstore-apps")
    except:
        pass
