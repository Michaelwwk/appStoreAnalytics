import os
import time
import subprocess
import pandas as pd
import numpy as np
import json
import re
import random
import requests
from google.cloud import bigquery
from tqdm import tqdm
from bs4 import BeautifulSoup
from pyspark.sql.types import *
from common import to_gbq, split_df
from dataSources.deleteRowsAppleGoogle import rawDataset, appleScraped_table_name, appleReview_table_name
from concurrent.futures import ThreadPoolExecutor
import warnings
warnings.filterwarnings('ignore')
import logging
logging.basicConfig(level=logging.ERROR)

# Hard-coded variables
appleAppsSample = 999 # 999 = all samples!
saveReviews = True
appleReviewCountPerApp = 20 # max 20
requests_per_second = 2 # None = turn off throttling!
retries = False # True = On, False = Off; Switching On may incur much longer computational time!
country = 'us'
# language = 'en'

def dataIngestionApple(client, project_id, noOfSlices = 1, subDf = 1):

    # Record the overall start time
    overall_start_time = time.time()

    appleScraped_db_path = f"{project_id}.{rawDataset}.{appleScraped_table_name}"
    appleReview_db_path = f"{project_id}.{rawDataset}.{appleReview_table_name}"

    # Apple
    ## Clone the repository
    subprocess.run(["git", "clone", "https://github.com/gauthamp10/apple-appstore-apps.git"])
    ## Change directory to the dataset folder
    os.chdir("apple-appstore-apps/dataset")
    ## Extract the tar.lzma file
    subprocess.run(["tar", "-xvf", "appleAppData.json.tar.lzma"])
    ## Read into DataFrame
    apple = pd.read_json("appleAppData.json")

    # Data Ingestion using 'app_store_scraper' API:

    apple_main = pd.DataFrame(columns = ['name', 'description', 'applicationCategory', 'datePublished',
                                          'operatingSystem', 'authorname', 'authorurl', 'ratingValue', 'reviewCount', 'price',
                                          'priceCurrency', 'star_ratings', 'appId'])
    
    apple_reviews_no_devResponse = pd.DataFrame(columns = ['id', 'type', 'offset', 'n_batch', 'app_id', 'attributes.date',
                                            'attributes.review', 'attributes.rating', 'attributes.isEdited',
                                            'attributes.userName', 'attributes.title'])
    
    apple_reviews_devResponse = pd.DataFrame(columns = ['id', 'type', 'offset', 'n_batch', 'app_id', 'attributes.date',
                                        'attributes.review', 'attributes.rating', 'attributes.isEdited',
                                        'attributes.userName', 'attributes.title',
                                        'attributes.developerResponse.id', 'attributes.developerResponse.body',
                                        'attributes.developerResponse.modified'])

    if appleAppsSample != 999:
        apple = apple.sample(appleAppsSample)
    
    if requests_per_second != None:
        delay_between_requests = 1 / requests_per_second
    else:
        delay_between_requests = None

    # Data Ingestion using web scraping

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

    def get_token(country:str , app_name:str , app_id: str, user_agents: dict):

        """
        Retrieves the bearer token required for API requests
        Regex adapted from base.py of https://github.com/cowboy-bebug/app-store-scraper
        """

        response = requests.get(f'https://apps.apple.com/{country}/app/{app_name}/id{app_id}', 
                                headers = {'User-Agent': random.choice(user_agents)},
                                )

        tags = response.text.splitlines()
        for tag in tags:
            if re.match(r"<meta.+web-experience-app/config/environment", tag):
                token = re.search(r"token%22%3A%22(.+?)%22", tag).group(1)
        
        return token
        
    def fetch_reviews(country:str , app_name:str , app_id: str, user_agents: dict, token: str, offset: str = '1', appleReviewCountPerApp = 20):

        """
        Fetches reviews for a given app from the Apple App Store API.

        - Default sleep after each call to reduce risk of rate limiting
        - Retry with increasing backoff if rate-limited (429)
        - No known ability to sort by date, but the higher the offset, the older the reviews tend to be
        """

        ## Define request headers and params
        landingUrl = f'https://apps.apple.com/{country}/app/{app_name}/id{app_id}'
        requestUrl = f'https://amp-api.apps.apple.com/v1/catalog/{country}/apps/{app_id}/reviews'

        headers = {
            'Accept': 'application/json',
            'Authorization': f'bearer {token}',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://apps.apple.com',
            'Referer': landingUrl,
            'User-Agent': random.choice(user_agents)
            }

        params = (
            ('l', 'en-GB'),                           # language
            ('offset', str(offset)),                  # paginate this offset
            ('limit', str(appleReviewCountPerApp)),   # max valid is 20
            ('platform', 'web'),
            ('additionalPlatforms', 'appletv,ipad,iphone,mac')
            )

        ## Perform request & exception handling
        retry_count = 0
        MAX_RETRIES = 1 # 5
        BASE_DELAY_SECS = 10
        # Assign dummy variables in case of GET failure
        result = {'data': [], 'next': None}
        reviews = []

        while retry_count < MAX_RETRIES:

            # Perform request
            response = requests.get(requestUrl, headers=headers, params=params)

            # SUCCESS
            # Parse response as JSON and exit loop if request was successful
            if response.status_code == 200:
                result = response.json()
                reviews = result['data']
                # if len(reviews) < 20:
                #     print(f"{len(reviews)} reviews scraped. This is fewer than the expected 20.")
                break

            # FAILURE
            elif response.status_code != 200:
                # print(f"GET request failed. Response: {response.status_code} {response.reason}")

                # RATE LIMITED
                if response.status_code == 429:

                    if retries:
                        # Perform backoff using retry_count as the backoff factor
                        retry_count += 1
                        backoff_time = BASE_DELAY_SECS * retry_count
                        print(f"Rate limited! Retrying ({retry_count}/{MAX_RETRIES}) after {backoff_time} seconds...")
                        
                        with tqdm(total=backoff_time, unit="sec", ncols=50) as pbar:
                            for _ in range(backoff_time):
                                time.sleep(1)
                                pbar.update(1)
                        continue
                    else:
                        print(f"Rate limited! Skipping for app Id: {app_id}.")
                        break

                # NOT FOUND
                elif response.status_code == 404:
                    # print(f"{response.status_code} {response.reason}. There are no more reviews.")
                    break

        ## Final output
        # Get pagination offset for next request
        if 'next' in result and result['next'] is not None:
            offset = re.search("^.+offset=([0-9]+).*$", result['next']).group(1)
            # print(f"Offset: {offset}")
        else:
            offset = None
            # print("No offset found.")

        # Append offset, number of reviews in batch, and app_id
        for rev in reviews:
            rev['offset'] = offset
            rev['n_batch'] = len(reviews)
            rev['app_id'] = app_id

        # Default sleep to decrease rate of calls
        time.sleep(0.5)
        return reviews

    def extract_app_id(url):
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
                return match.group(1)
            else:
                return None
        else:
            return None
        
    def process_app(appId):
        app_results = appWithThrottle(
                                appId = appId,
                                country=country,
                                delay_between_requests = delay_between_requests
                                )
        
        if app_results['name'] != 'App Store':
            successAppId = appId
            row = [value for value in app_results.values()]
            row.append(successAppId)
            apple_main.loc[len(apple_main)] = row
        
            if saveReviews == True:

                token = get_token(country, 'anything', successAppId, user_agents)
                reviews = fetch_reviews(country, 'anything', successAppId, user_agents, token, appleReviewCountPerApp = appleReviewCountPerApp)
                df = pd.json_normalize(reviews)
                
                df_list = df.values.tolist()
                if len(df.columns) == 11:
                    for index in range(0, len(df_list)):
                        try:
                            apple_reviews_no_devResponse.loc[len(apple_reviews_no_devResponse)] = df_list[index]
                        except Exception as e:
                            print(f"Apple (Appending Error): {appId} -> {e}. apple_reviews_no_devResponse shape: {apple_reviews_no_devResponse.shape}, \
df_list[index] length: {len(df_list[index])}")
                elif len(df.columns) == 14: #14
                    for index in range(0, len(df_list)):
                        try:
                            apple_reviews_devResponse.loc[len(apple_reviews_devResponse)] = df_list[index]
                        except Exception as e:
                            print(f"Apple (Appending Error): {appId} -> {e}. apple_reviews_devResponse shape: {apple_reviews_devResponse.shape}, \
df_list[index] length: {len(df_list[index])}")
                            
        else:
            print(f"Apple: {appId} -> App not found.")
                
    user_agents = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
    ]

    apple['AppStore_Url'] = apple['AppStore_Url'].apply(extract_app_id)
    apple.drop_duplicates(subset = ['AppStore_Url'], keep = 'first', inplace = True)
    apple = split_df(apple, noOfSlices = noOfSlices, subDf = subDf)

    appsChecked = 0
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        print(f"No. of worker threads deployed: {os.cpu_count()}")

        for appId in apple.iloc[:, 2]:

            # Record the end time
            overall_end_time = time.time()         
            # Calculate and print the overall elapsed time in seconds
            overall_elapsed_time = overall_end_time - overall_start_time

            if overall_elapsed_time < 21000: # 5 hour 50 mins

                appsChecked += 1

                try:
                    # Record the start time
                    start_time = time.time()

                    executor.submit(process_app, appId)

                    # Record the end time
                    end_time = time.time()         
                    # Calculate and print the elapsed time in seconds
                    elapsed_time = end_time - start_time

                    # if appId in apple_main['appId'].to_list():
                    print(f'Apple: {appId} -> Successfully saved in {elapsed_time} seconds. Total -> \
{appsChecked}/{len(apple)} ({round(appsChecked/len(apple)*100,1)}%) completed.')

                except Exception as e:
                    print(f"Apple (Error): {appId} -> {e}")
            
            else:
                print("Exiting data ingestion prematurely ..")
                break

    apple_reviews = pd.concat([apple_reviews_devResponse, apple_reviews_no_devResponse], ignore_index=True)

    # Rename columns
    apple_reviews.columns = ['id', 'type', 'offset', 'nBatch', 'appId', 'date', 'review', 'rating', 'isEdited', 'userName', 'title',
                            'developerResponseId', 'developerResponseBody', 'developerResponseModified']

    # Create tables into Google BigQuery
    client.create_table(bigquery.Table(appleScraped_db_path), exists_ok = True)
    client.create_table(bigquery.Table(appleReview_db_path), exists_ok = True)

    # Push data into DB
    to_gbq(apple_main, rawDataset, appleScraped_table_name, mergeType = 'WRITE_APPEND', sparkdf = False, allDataTypes = False)
    to_gbq(apple_reviews, rawDataset, appleReview_table_name, mergeType = 'WRITE_APPEND', sparkdf = False, allDataTypes = False)
    # ^ this raw table will have duplicates; drop the duplicates before pushing to clean table!!

    # Completion log
    if noOfSlices != 0:
        print(f"Data ingestion step completed using this runner. \
{len(apple_main.appId.unique())}/{len(apple.App_Id.unique())} ({round(len(apple_main.appId.unique())/len(apple.App_Id.unique())*100,1)}%) apps matched. \
{apple_reviews.appId.nunique()}/{apple_main.appId.nunique()} ({round(apple_reviews.appId.nunique()/apple_main.appId.nunique()*100,1)}%) apps has reviews.")
        print(f"{appleScraped_db_path} & {appleReview_db_path} raw tables partially updated.")