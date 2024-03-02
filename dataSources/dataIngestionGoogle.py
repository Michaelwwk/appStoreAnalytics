import os
import time
import subprocess
import glob
import shutil
import pandas as pd
import json
from google.cloud import bigquery
from google_play_scraper import app, reviews, Sort
from pyspark.sql.types import *
from commonFunctions import to_gbq, split_df
from dataSources.deleteRowsAppleGoogle import googleScraped_table_name, googleReview_table_name
import warnings
warnings.filterwarnings('ignore')

# # Function to convert pandas DataFrame to Spark DataFrame
# def pandas_to_spark(df, spark):
#     return spark.createDataFrame(df)

# # Function to write Spark DataFrame to BigQuery
# def write_spark_to_bigquery(spark_df, table_name, dataset_name, project_id):
#     spark_df.write.format('bigquery') \
#         .option('table', f'{project_id}.{dataset_name}.{table_name}') \
#         .save()

def dataIngestionGoogle(noOfSlices = 1, subDf = 1):
    
    # Set folder path
    folder_path = os.path.abspath(os.path.expanduser('~')).replace("\\", "/")
    folder_path = f"{folder_path}/work/appStoreAnalytics/appStoreAnalytics"
    googleAPI_json_path = f"{folder_path}/googleAPI.json"
    log_file_path = f"{folder_path}/dataSources/googleDataIngestion.log"

    # Extract Google API from GitHub Secret Variable
    googleAPI_dict = json.loads(os.environ["GOOGLEAPI"])
    with open(googleAPI_json_path, "w") as f:
        json.dump(googleAPI_dict, f)

    # Hard-coded variables
    googleAppsSample = 4000 # 999 = all samples!
    saveReviews = True
    reviewCountPerApp = 40
    requests_per_second = None # None = turn off throttling!
    country = 'us'
    language = 'en'
    project_id =  googleAPI_dict["project_id"]
    rawDataset = "rawData"
    googleScraped_db_dataSetTableName = f"{rawDataset}.{googleScraped_table_name}"
    googleScraped_db_path = f"{project_id}.{rawDataset}.{googleScraped_table_name}"
    googleReview_db_dataSetTableName = f"{rawDataset}.{googleReview_table_name}"
    googleReview_db_path = f"{project_id}.{rawDataset}.{googleReview_table_name}"

    client = bigquery.Client.from_service_account_json(googleAPI_json_path, project = project_id)

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
    google = pd.read_csv("Google-Playstore-Dataset.csv", header = None) # low_memory = False
    # column_names = ['App Name', 'App Id', 'Category', 'Rating', 'Rating Count', 'Installs',
    #                 'Minimum Installs', 'Maximum Installs', 'Free', 'Price', 'Currency',
    #                 'Size', 'Minimum Android', 'Developer Id', 'Developer Website',
    #                 'Developer Email', 'Released', 'Last Updated', 'Content Rating',
    #                 'Privacy Policy', 'Ad Supported', 'In App Purchases', 'Editors Choice',
    #                 'Scraped Time']
    # google.columns = column_names

    print(google.columns)
    print(google.shape)
    print(google.head())

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

    reviewCountRange = range(0,reviewCountPerApp)

    if googleAppsSample != 999:
        google = google.sample(googleAppsSample)

    try:
        os.remove(log_file_path)
    except:
        pass
    
    if requests_per_second != None:
        delay_between_requests = 1 / requests_per_second
    else:
        delay_between_requests = None

    def appWithThrottle(appId, lang = 'en', country = 'us', delay_between_requests = None):
        output = app(
                    appId,
                    lang=lang, # defaults to 'en'
                    country=country # defaults to 'us'
                    )
        if delay_between_requests != None:
            time.sleep(delay_between_requests)
        return output
    
    def reviewsWithThrottle(appId, lang = 'en', country = 'us', count = 100, score = None, delay_between_requests = None):
        output = reviews(
                        appId,
                        lang=lang, # defaults to 'en'
                        country=country, # defaults to 'us'
                        sort=Sort.NEWEST, # defaults to Sort.NEWEST
                        count=count, # defaults to 100
                        filter_score_with=score # defaults to None(means all score)
                        )
        if delay_between_requests != None:
            time.sleep(delay_between_requests)
        return output

    appsChecked = 0
    google.drop_duplicates(subset = ['App_Id'], keep = 'first', inplace = True)
    google = split_df(google, noOfSlices = noOfSlices, subDf = subDf)
    for appId in google.iloc[:, 1]:
        appsChecked += 1
        appReviewCounts = 0

        try:
            app_results = appWithThrottle(
                                    appId,
                                    lang=language,
                                    country=country,
                                    delay_between_requests = delay_between_requests
                                    )

            for feature in mainFeaturesToDrop:
                app_results.pop(feature, None)
            row = [value for value in app_results.values()]
            google_main.loc[len(google_main)] = row

            if saveReviews == True:

                # for score in range(1,6):
                review, continuation_token = reviewsWithThrottle(
                    appId,
                    lang=language,
                    country=country,
                    count=reviewCountPerApp,
                    score=None,
                    delay_between_requests = delay_between_requests
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
            
            # with open(log_file_path, "a") as log_file:
               # log_file.write(f"{appId} -> Successfully saved with {appReviewCounts} review(s). Total: {len(google_main)} app(s) & {len(google_reviews)} review(s) saved.\n")
            print(f'Google: {appId} -> Successfully saved with {appReviewCounts} review(s). Total -> {len(google_main)}/{appsChecked} app(s) & {len(google_reviews)} review(s) saved. {appsChecked}/{len(google)} ({round(appsChecked/len(google)*100,1)}%) completed.')
        except Exception as e:
            # with open(log_file_path, "a") as log_file:
                # log_file.write(f"{appId} -> Error occurred: {e}\n")
            print(f"Google: {e}")
            
    # Create tables into Google BigQuery
    client.create_table(bigquery.Table(googleScraped_db_path), exists_ok = True)
    client.create_table(bigquery.Table(googleReview_db_path), exists_ok = True)

    # Push data into DB
    google_main = google_main.astype(str) # all columns will be string
    load_job = to_gbq(google_main, client, googleScraped_db_dataSetTableName)
    load_job.result()

    google_reviews = google_reviews.astype(str) # all columns will be string
    load_job = to_gbq(google_reviews, client, googleReview_db_dataSetTableName, mergeType = 'WRITE_APPEND') # this raw table will have duplicates; drop the duplicates before pushing to clean table!!
    load_job.result()

    ## Remove files and folder
    try:
        os.remove(googleAPI_json_path)
        shutil.rmtree(f"{folder_path}apple-appstore-apps")
    except:
        pass
