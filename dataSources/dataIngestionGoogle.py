import os
import time
import subprocess
import glob
import pandas as pd
from google.cloud import bigquery
from google_play_scraper import app, reviews, Sort
from pyspark.sql.types import *
from common import to_gbq, split_df
from dataSources.deleteRowsAppleGoogle import googleScraped_table_name, googleReview_table_name
import warnings
warnings.filterwarnings('ignore')

def dataIngestionGoogle(client, project_id, noOfSlices = 1, subDf = 1):

    # Hard-coded variables
    googleAppsSample = 100 # 999 = all samples!
    saveReviews = True
    reviewCountPerApp = 20 # 40
    requests_per_second = None # None = turn off throttling!
    country = 'us'
    language = 'en'
    project_id =  project_id
    rawDataset = "rawData"
    googleScraped_db_dataSetTableName = f"{rawDataset}.{googleScraped_table_name}"
    googleScraped_db_path = f"{project_id}.{rawDataset}.{googleScraped_table_name}"
    googleReview_db_dataSetTableName = f"{rawDataset}.{googleReview_table_name}"
    googleReview_db_path = f"{project_id}.{rawDataset}.{googleReview_table_name}"

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
    column_names = ['App Name', 'App Id', 'Category', 'Rating', 'Rating Count', 'Installs',
                    'Minimum Installs', 'Maximum Installs', 'Free', 'Price', 'Currency',
                    'Size', 'Minimum Android', 'Developer Id', 'Developer Website',
                    'Developer Email', 'Released', 'Last Updated', 'Content Rating',
                    'Privacy Policy', 'Ad Supported', 'In App Purchases', 'Editors Choice',
                    'Scraped Time']
    google.columns = column_names
    google = google[~((google['App Name'] == 'App Name') & (google['App Id'] == 'App Id'))]

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

    google.drop_duplicates(subset = ['App Id'], keep = 'first', inplace = True)
    google = split_df(google, noOfSlices = noOfSlices, subDf = subDf)

    appsChecked = 0
    for appId in google.iloc[:, 1]:

        # Record the start time
        start_time = time.time()

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
            
            print(f'Google: {appId} -> Successfully saved with {appReviewCounts} review(s). Total -> {len(google_main)}/{appsChecked} app(s) \
& {len(google_reviews)} review(s) saved. {appsChecked}/{len(google)} ({round(appsChecked/len(google)*100,1)}%) completed.')
        except Exception as e:
            print(f"Google: {appId} ->: {e}")

        # Record the end time
        end_time = time.time()         
        # Calculate and print the elapsed time in seconds
        elapsed_time = end_time - start_time
        print(f"({appId} -> {elapsed_time} seconds)")
            
        # Create tables into Google BigQuery
        client.create_table(bigquery.Table(googleScraped_db_path), exists_ok = True)
        client.create_table(bigquery.Table(googleReview_db_path), exists_ok = True)

    # Push data into DB
    load_job = to_gbq(google_main, googleScraped_db_dataSetTableName, mergeType = 'WRITE_APPEND')
    load_job.result()

    load_job = to_gbq(google_reviews, googleReview_db_dataSetTableName, mergeType = 'WRITE_APPEND')
    # ^ this raw table will have duplicates; drop the duplicates before pushing to clean table!!
    load_job.result()