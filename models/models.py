import os
import nltk
from common import read_gbq, to_gbq, googleAPI_json_path
from dataWrangling.dataWrangling import cleanDataset, cleanGoogleMainScraped_table_name, cleanAppleMainScraped_table_name
from google.cloud import bigquery
import pygsheets
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from nltk.tokenize import word_tokenize
from pyspark.sql.functions import split, lower, concat, concat_ws, col, lit
from pyspark.ml.feature import Tokenizer
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
nltk.download('punkt')

# Hard-coded variables
modelDataset = "dev_modelData"
modelAppleScraped_table_name = 'modelAppleMain' # TODO just for example!
modelGoogleScraped_table_name = 'modelGoogleMain' # TODO just for example!
appleRecommendationModel_table_name = 'modelAppleRecommendation'
googleRecommendationModel_table_name = 'modelGoogleRecommendation'
googleSheetURL = "https://docs.google.com/spreadsheets/d/1zo96WvtgcfznAmSjlQJpnbKIX_NfSIMpsdLcrJOYctw/edit#gid=0"
appName = "What is the name of your new application?"
appDescription = "Please provide a description for your application."
appSummary = "Please provide a short summary for your application."
appStore = "Will you be publishing your application to Apple App Store, Google Play Store, or both?"

# Define the scope of the Google Sheets API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# Authenticate with Google Sheets API using the service account key
gspread_client = pygsheets.authorize(service_file = googleAPI_json_path)

# Read Google Sheet
spreadsheet = gspread_client.open_by_url(googleSheetURL)
worksheet = spreadsheet.sheet1
data = worksheet.get_all_records()

# TODO Follow this template when scripting!!
def appleClassificationModel(spark, project_id, client):
    return

def googleClassificationModel(spark, project_id, client):
    
    modelGoogleScraped_db_path = f"{project_id}.{modelDataset}.{modelGoogleScraped_table_name}"

    sparkDf = read_gbq(spark, cleanDataset, cleanGoogleMainScraped_table_name)
    # print(sparkDf.show())
    print(sparkDf.count())

    client.create_table(bigquery.Table(modelGoogleScraped_db_path), exists_ok = True)
    to_gbq(sparkDf, modelDataset, modelGoogleScraped_table_name)

def recommendationModel(spark, sparkDf, apple_google, apple_google_store, text_tokens): # TODO remove text_tokens parameter!

    folder_path = os.getcwd().replace("\\", "/")
    recModelFile_path = f"{folder_path}/models/{apple_google}RecModel.model"

    # TO DELETE (START) #

    # # Tokenize the text and store it in the "text_tokens" column
    # sparkDf = sparkDf.withColumn("text_tokens", split(lower("text"), "\s+"))
    # # Select only the "text_tokens" column and collect it into a list
    # text_tokens = sparkDf.select("text_tokens").rdd.flatMap(lambda x: x).collect()

    # Load data into model
    print("Loading data into model ..")
    tagged_data = [TaggedDocument(d, [i]) for i, d in enumerate(text_tokens)]
    model = Doc2Vec(vector_size=64, min_count=2, epochs=40)
    model.build_vocab(tagged_data)
    print("Data loaded into model.")

    # Train the model using our data
    print("Training model ..")
    model.train(tagged_data, total_examples=model.corpus_count, epochs=40)
    print("Model trained!")

    #The model can be saved for future usage
    model.save(recModelFile_path)
    print(f"{apple_google} recommendation model saved.")

    # TO DELETE (END) #

    newApplications_df = spark.createDataFrame(data)
    newApplications_df = newApplications_df.filter(
    (newApplications_df[appStore] == apple_google_store) |
    (newApplications_df[appStore] == "Both")
    )

    # Define the schema
    schema = StructType([
        StructField("newApp", StringType(), nullable=True),
        StructField("appRank", StringType(), nullable=True),
        StructField("appName", StringType(), nullable=True),
        StructField("appId", StringType(), nullable=True),
        StructField("appScore", DoubleType(), nullable=True)
    ])

    # Create an empty DataFrame with the specified schema
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    # Concatenate the columns into a single column "text"
    newApplications_df = newApplications_df.withColumn(
        "text",
        concat_ws(" ", 
                lower(col(appName)), 
                lower(col(f"`{appDescription}`")), 
                lower(col(f"`{appSummary}`")))
    )

    # Tokenize the text column
    tokenizer = Tokenizer(inputCol="text", outputCol="text_tokens")
    newApplications_df = tokenizer.transform(newApplications_df)

    # Load saved doc2vec model
    model= Doc2Vec.load(recModelFile_path)

    # Iterate over each row and compute similarity scores
    for row in newApplications_df.collect():
        test_vec = model.infer_vector(row["text_tokens"])
        results = model.docvecs.most_similar(positive=[test_vec], topn=5)
        print("[Original]\n")
        print(f"Title: {row[appName]}")
        print(f"Description: {row[appDescription]}")
        print(f"Summary: {row[appSummary]}")
        print("-" * 50)
        
        # Iterate over the results and append rows to the DataFrame
        for i, (doc_id, similarity_score) in enumerate(results):

            if apple_google == "apple":
                title = sparkDf.select("name").collect()[doc_id][0]
            else:
                title = sparkDf.select("title").collect()[doc_id][0]
            id = sparkDf.select("appId").collect()[doc_id][0]
            text = sparkDf.select("text").collect()[doc_id][0]
            print(f"[Result {i+1}]\n")
            print(f"Score:\n{similarity_score}\n")
            print(f"Title:\n{title}\n")
            print(f"Details:\n{text}\n")
            print("-" * 50)
            
            # Create a new row
            new_row = (row[appName], str(i+1), title, id, similarity_score)
            
            # Append the row to the DataFrame
            df = df.union(spark.createDataFrame([new_row], schema=schema))

    # Filter out the empty row
    df = df.filter(df.newApp.isNotNull())

    return df

def appleRecommendationModel(spark, project_id, client):

    recommendationModel_table_name_db_path = f"{project_id}.{modelDataset}.{appleRecommendationModel_table_name}"
    pandasDf = read_gbq(spark, cleanDataset, cleanAppleMainScraped_table_name, sparkDf = False) # TODO to delete!
    print("Apple pandasDf loaded.") # TODO to delete!
    sparkDf = read_gbq(spark, cleanDataset, cleanAppleMainScraped_table_name)
    print("Apple sparkDf loaded.")

    # Combine 'name' and 'description' columns into a single column 'textonly'
    pandasDf['textonly'] = pandasDf['name'] + ' ' + pandasDf['description'] # TODO to delete!
    # Handle missing values by replacing them with empty strings
    textonly = pandasDf['textonly'].fillna('') # TODO to delete!
    # Tokenize the text in the 'textonly' column
    text_tokens = [word_tokenize(t.lower()) for t in textonly] # TODO to delete!

    sparkDf = sparkDf.withColumn('text', concat(col('name'), lit(' '), col('description')))
    df = recommendationModel(spark, sparkDf, apple_google = 'apple', apple_google_store = 'Apple App Store', text_tokens = text_tokens) # TODO remove text_tokens parameter!
    
    client.create_table(bigquery.Table(recommendationModel_table_name_db_path), exists_ok = True)
    to_gbq(df, modelDataset, appleRecommendationModel_table_name)

def googleRecommendationModel(spark, project_id, client):

    recommendationModel_table_name_db_path = f"{project_id}.{modelDataset}.{googleRecommendationModel_table_name}"
    pandasDf = read_gbq(spark, cleanDataset, cleanGoogleMainScraped_table_name, sparkDf = False) # TODO to delete!
    print("Google pandasDf loaded.") # TODO to delete!
    sparkDf = read_gbq(spark, cleanDataset, cleanGoogleMainScraped_table_name)
    print("Google sparkDf loaded.")

    # Combine 'title', 'description', and 'summary' columns into a single column 'textonly'
    pandasDf['textonly'] = pandasDf['title'] + ' ' + pandasDf['description'] + ' ' + pandasDf['summary'] # TODO to delete!
    # Handle missing values by replacing them with empty strings
    textonly = pandasDf['textonly'].fillna('') # TODO to delete!
    # Tokenize the text in the 'textonly' column
    text_tokens = [word_tokenize(t.lower()) for t in textonly] # TODO to delete!

    sparkDf = sparkDf.withColumn('text', concat(col('title'), lit(' '), col('description'), lit(' '), col('summary')))
    df = recommendationModel(spark, sparkDf, apple_google = 'google', apple_google_store = 'Google Play Store', text_tokens = text_tokens) # TODO remove text_tokens parameter!
    
    client.create_table(bigquery.Table(recommendationModel_table_name_db_path), exists_ok = True)
    to_gbq(df, modelDataset, googleRecommendationModel_table_name)