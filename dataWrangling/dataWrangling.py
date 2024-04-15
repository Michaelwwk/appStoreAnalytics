from functools import reduce
from common import read_gbq, to_gbq
import ast
import time
from google.cloud import bigquery
from dataSources.deleteRowsAppleGoogle import rawDataset, googleScraped_table_name
from pyspark.sql.functions import col, regexp_replace, split, expr, udf, when, lit
from pyspark.sql.types import ArrayType, StructType, StructField, FloatType, StringType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.sql.functions import col, udf
from pyspark.ml import Pipeline


# Language Detection Packages
# 0. Generic
    # import nltk
    # from nltk.corpus import words
    # from nltk.tokenize import word_tokenize
    # nltk.download('words')
    # nltk.download('punkt')

# # 1. Polyglot
# from polyglot.detect import Detector

# 2. Langdetect 👍
from langdetect import detect

# # 3. Gcld3 (Google Compact Language Detector 3)
# import gcld3



# Hard-coded variables
cleanDataset = "dev_cleanData" # Schema/Dataset
cleanGoogleMainScraped_table_name = 'cleanGoogleMain' # Table
cleanGoogleReviewScraped_table_name = 'cleanGoogleReview' # Table
cleanAppleMainScraped_table_name = 'cleanAppleMain' # Table
cleanAppleReviewScraped_table_name = 'cleanAppleReview' # Table

rawDataset = "dev_rawData"
googleMain = "googleMain" # Table
googleReview = "googleReview" # Table
appleMain = "appleMain" # Table
appleReview = "appleReview" # Table

# TODO Follow this template when scripting!!

# Apple functions
def remove_string_from_column_apple(column, string):
    return regexp_replace(col(column), string, "")

def helper_apple(df, string, column):
    return df.withColumn(column, remove_string_from_column_apple(column, string))

def remove_strings_from_df_apple(df, column, strings):
    return reduce(lambda acc, string: helper_apple(acc, string, column), strings, df)

def remove_strings_from_columns_apple(df, strings_to_remove):
    return reduce(lambda acc, column: remove_strings_from_df_apple(acc, column, strings_to_remove[column]), strings_to_remove, df)

def remove_strings_apple(content, strings_to_remove):
    return reduce(lambda acc, s: acc.replace(s, ""), strings_to_remove, content)

def detect_language_langdetect_apple(text):
    try:
        return detect(text)
    except:
        return "unknown"


# Apple Data Wrangling
def clean_data_appleMain(df):
    # Drop specific columns
    columns_to_drop = ['operatingSystem', 'authorurl']
    df = df.drop(*columns_to_drop)

    # Remove specified strings from specified columns
    strings_to_remove = {
    'description': ['<b>']
    }
    
    df = remove_strings_from_columns_apple(df, strings_to_remove)
    
    # Transform star_ratings column
    # Remove characters not needed [, ], (, ), ', %
    df = df.withColumn('cleaned_star_ratings', regexp_replace('star_ratings', r"[(|)|'|\]|\[|%]", ""))
    
    splitCol = split(df['cleaned_star_ratings'], ',')
    
    df = df.withColumn('5starrating_no', splitCol.getItem(1).cast('int')) \
        .withColumn('4starrating_no', splitCol.getItem(3).cast('int')) \
            .withColumn('3starrating_no', splitCol.getItem(5).cast('int')) \
                .withColumn('2starrating_no', splitCol.getItem(7).cast('int')) \
                    .withColumn('1starrating_no', splitCol.getItem(9).cast('int'))
                    
    df = df.drop('star_ratings').drop('cleaned_star_ratings')
    
    # Remove &amp; in applicationCategory
    df = df.withColumn('applicationCategory', regexp_replace('applicationCategory', r"&amp;", "&"))
    
    # Add new columns that aligns with googleMain
    df = df.withColumn('summary', lit(None).cast(StringType())) \
        .withColumn('installs', lit(None).cast(StringType())) \
            .withColumn('minInstalls', lit(None).cast(StringType())) \
                .withColumn('realInstalls', lit(None).cast(StringType())) \
                    .withColumn('ratings', lit(None).cast(StringType())) \
                        .withColumn('free', lit(None).cast(StringType())) \
                            .withColumn('offersIAP', lit(None).cast(StringType())) \
                                .withColumn('inAppProductPrice', lit(None).cast(StringType())) \
                                    .withColumn('genreId', lit(None).cast(StringType())) \
                                        .withColumn('contentRating', lit(None).cast(StringType())) \
                                            .withColumn('contentRatingDescription', lit(None).cast(StringType())) \
                                                .withColumn('adSupported', lit(None).cast(StringType())) \
                                                    .withColumn('lastUpdatedOn', lit(None).cast(StringType())) \
                                                        .withColumn('version', lit(None).cast(StringType())) \
                                                            .withColumn('categories_list', lit(None).cast(StringType())) \
                                                                .withColumn('min_inAppProductPrice', lit(None).cast(StringType())) \
                                                                    .withColumn('max_inAppProductPrice', lit(None).cast(StringType()))
    
    # Change and arrange column names to align with googleMain
    df = df.selectExpr('name as title', 
                       'description', 
                       'summary', 'installs', 'minInstalls', 'realInstalls', 
                       'ratingValue as score', 
                       'ratings', 
                       'reviewCount as reviews', 
                       'price', 
                       'free', 
                       'priceCurrency as currency', 
                       'offersIAP', 'inAppProductPrice', 
                       'authorname as developer', 
                       'applicationCategory as genre', 
                       'genreId', 'contentRating', 'contentRatingDescription', 'adSupported', 
                       'datePublished as released', 
                       'lastUpdatedOn', 'version', 
                       'appId', 
                       'categories_list', 'min_inAppProductPrice', 'max_inAppProductPrice', 
                       '1starrating_no', 
                       '2starrating_no', 
                       '3starrating_no', 
                       '4starrating_no', 
                       '5starrating_no')
    
    # Drop records where 'ratingValue' column has a value of nan
    df = df.filter((col("ratingValue") != 'nan'))
    
    return df

def clean_data_appleReview(spark, df, ref_appid_sparkDf):
    # Drop specific columns
    # No columns to drop
    
    # Filter rows where there is a mismatch in data with header columns
    df = df.filter((col("developerResponseId") == 'nan') & (col("developerResponseBody") == 'nan') & (col("developerResponseModified") == 'nan'))
    
    # Filter only records where app_id in df is in ref_appid_sparkDf.select("app_Id").distinct()
    df = df.join(ref_appid_sparkDf.select("appId").distinct(), "appId", "inner")
    
    # Drop duplicates
    df = df.dropDuplicates(['id'])
    
    # Rename and rearrnge columns to align with googleReview and drop type, offset, nBatch columns
    df = df.selectExpr('appId', 
                       'id as reviewId', 
                       'userName', 
                       'review as content', 
                       'rating as score', 
                       'date as at', 
                       'developerResponseBody as replyContent',
                       'isEdited', 'title', 'developerResponseId', 'developerResponseModified')

    # Remove specific strings from specific columns
    # Running into an error - commenting out first.
    strings_to_remove = ["<b>"]
    
    remove_strings_udf = udf(lambda x: remove_strings_apple(x, strings_to_remove), StringType())
    
    df = df.withColumn("content", remove_strings_udf(col("content")))
    
    # 2. Langdetect
    # Define a UDF to apply language detection
    detect_language_udf = spark.udf.register("detect_language_udf", detect_language_langdetect_apple)
    
    # Apply language detection to the 'content' column
    df = df.withColumn("language_langdetect", detect_language_udf("content"))

    # Filter only language_langdetect = 'en'
    df = df.filter(df['language_langdetect'] == 'en')

    #TA
    custom_stopwords = ["don", "should", "now", "need", "working", "without", "doge", "screen", "app.",
    "first", "cant", "completely", "won't", "make", "still", "definitions",  "i'm", "many",
    "want", "game", "don't", "even", "can't", "doesn't", "worst", "it's",
    "one", "open", "work", "get", "people", "like", "good",  "nothing", "every", "would", "words",
    "actually", "the", "and", "to", "i", "app", "a", "of", "not", "it", "this", "is", "in", "your", "you", "very", "but", "for", "are", "they", "time", "have", "no", "please", "with", "so", "that", "bad",
    "app","will","ok","u","please","great","i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your" "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",
    "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",
    "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do",
    "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
    "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
    "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
    "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
    "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
    "too", "very", "s", "t", "can", "will", "just", "don", "should", "now","thank","sorry","could","you."]

    # Create TA Pipeline
    tokenizer = Tokenizer(inputCol="content", outputCol="tokens")
    stopwords_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_tokens", stopWords=custom_stopwords)
  
    pipeline = Pipeline(stages=[tokenizer, stopwords_remover])
    TA_pipeline = pipeline.fit(df)

    #transformed_df holds the result of applying this pipeline to original df, performing tokenization, stop word removal
    df = TA_pipeline.transform(df)

    return df

def appleDataWrangling(spark, project_id, client, local = False, sparkDf = None, sparkRvDf = None):
    #################################################### appleMain
    if not local: 
        cleanAppleScraped_db_path = f"{project_id}.{cleanDataset}.{cleanAppleMainScraped_table_name}" # Schema + Table
        sparkDf = read_gbq(spark, rawDataset, appleMain)  # reinstate at production
        
        print(sparkDf.count())
    
    # Code section for cleaning googleMain data
    cleaned_sparkDf = clean_data_appleMain(sparkDf)
    
    if not local: 
        client.create_table(bigquery.Table(cleanAppleScraped_db_path), exists_ok = True)
        to_gbq(cleaned_sparkDf, cleanDataset, cleanAppleMainScraped_table_name)  # reinstate at production
    
    #################################################### appleReview
    if not local: 
        cleanAppleScraped_db_path = f"{project_id}.{cleanDataset}.{cleanAppleReviewScraped_table_name}" # Schema + Table
        sparkDf = read_gbq(spark, rawDataset, appleReview)  # reinstate at production
        
    else: 
        sparkDf = sparkRvDf
    
    print(sparkDf.count())

    time.sleep(30)
    
    if not local: 
        ref_appid_sparkDf = read_gbq(spark, cleanDataset, cleanAppleMainScraped_table_name)  # reinstate at production
        pass
    else: 
        ref_appid_sparkDf = cleaned_sparkDf
        
    # print(sparkDf.show())
    print(ref_appid_sparkDf.count())

    # Code section for cleaning googleMain data
    cleaned_sparkDf = clean_data_appleReview(spark, sparkDf, ref_appid_sparkDf)

    client.create_table(bigquery.Table(cleanAppleScraped_db_path), exists_ok = True)  # reinstate at production

    print('Table created successfully')

    to_gbq(cleaned_sparkDf, cleanDataset, cleanAppleReviewScraped_table_name)  # reinstate at production

    print('Table sent to GBQ successfully')
    
    return


# Google Data Wrangling
def googleDataWrangling(spark, project_id, client):

    # googleMain
    
    cleanGoogleScraped_db_path = f"{project_id}.{cleanDataset}.{cleanGoogleMainScraped_table_name}" # Schema + Table

    sparkDf = read_gbq(spark, rawDataset, googleMain)
    # print(sparkDf.show())
    print(sparkDf.count())

    # Code section for cleaning googleMain data
    def clean_data_googleMain(df):

        # Drop specific columns
        columns_to_drop = ['descriptionHTML', 'sale', 'saleTime', 'originalPrice', 'saleText', 'developerId', 'developerAddress', 'containsAds', 'updated']
        df = df.drop(*columns_to_drop)

        # Remove specified strings from specified columns
        strings_to_remove = {
        'description': ['<b>']
        }
        # for column, strings in strings_to_remove.items():
        #     for string in strings:
        #         df = df.withColumn(column, regexp_replace(col(column), string, ""))

        def remove_strings_from_columns(df, strings_to_remove):
            def remove_string_from_column(column, string):
                return regexp_replace(col(column), string, "")

            # def remove_strings_from_df(df, column, strings):
            #     for string in strings:
            #         df = df.withColumn(column, remove_string_from_column(column, string))
            #     return df
            
            def remove_strings_from_df(df, column, strings):
                def helper(df, string):
                    return df.withColumn(column, remove_string_from_column(column, string))
                
                return reduce(lambda acc, string: helper(acc, string), strings, df)


            return reduce(lambda acc, column: remove_strings_from_df(acc, column, strings_to_remove[column]), strings_to_remove, df)

        df = remove_strings_from_columns(df, strings_to_remove)





        # Convert categories list dictionary into list
        # Define the extract_names_udf function as a UDF (Purely Functional Programming)

        def extract_names_udf(data):
            elements = ast.literal_eval(data)
            return list(map(lambda item: item['name'], elements))

                        # def extract_names_udf(data):
                        #     elements = ast.literal_eval(data)
                        #     names = [item['name'] for item in elements] 
                        #     return names
        
        extract_names = udf(extract_names_udf, ArrayType(StringType()))
        
        # Apply the UDF to the "categories" column
        df = df.withColumn("categories_list", extract_names("categories"))
        # Convert the categories_list column to string datatype
        df = df.withColumn("categories_list", col("categories_list").cast(StringType()))
        df = df.drop("categories")
    
        
        # Drop records where 'ratings' column has a value of 0, nan or None
        df = df.filter((col("ratings") != 0) & (col("ratings") != 'None'))
                    #    & (~col("ratings") != 'nan') & (col("ratings") != 'None'))

        # Drop records where 'minInstalls' column is None
        df = df.filter((col("minInstalls") != 0) & (col("minInstalls") != 'None'))
                    #    & (~col("minInstalls") != 'nan') & (col("minInstalls") != 'None'))
        



        # # Splitting the column based on ' - ' and ' per item' and selecting the appropriate elements
        # df = df.withColumn('price_range', split(col('inAppProductPrice'), ' - '))
        # df = df.withColumn('min_inAppProductPrice', when(col('price_range').getItem(0).contains('$'), col('price_range').getItem(0).substr(2, 4)).otherwise(None))
        # df = df.withColumn('max_inAppProductPrice', when(col('price_range').getItem(1).contains('$'), col('price_range').getItem(1).substr(2, 4)).otherwise(None))

        # Splitting the column based on ' - ' and ' per item' and selecting the appropriate elements
        df = df.withColumn('price_range', split(col('inAppProductPrice'), ' - '))
        df = df.withColumn('min_inAppProductPrice', when(col('price_range').getItem(0).contains('$'), regexp_replace(col('price_range').getItem(0), '\\$', '').substr(1, 5)).otherwise(None))
        df = df.withColumn('max_inAppProductPrice', when(col('price_range').getItem(1).contains('$'), regexp_replace(col('price_range').getItem(1), '\\$', '').substr(1, 6)).otherwise(None))

        # Dropping the intermediate column and displaying the DataFrame
        df = df.drop('price_range')








        # Split 'histogram' column into 5 columns
        df = df.withColumn("histogram", expr("substring(histogram, 2, length(histogram) - 2)")) # Remove brackets []
        df = df.withColumn("histogram", split(col("histogram"), ", ")) # Split into array
        df = df.withColumn("1starrating_no", df.histogram.getItem(0).cast("int")) \
            .withColumn("2starrating_no", df.histogram.getItem(1).cast("int")) \
            .withColumn("3starrating_no", df.histogram.getItem(2).cast("int")) \
            .withColumn("4starrating_no", df.histogram.getItem(3).cast("int")) \
            .withColumn("5starrating_no", df.histogram.getItem(4).cast("int")) \
            .drop("histogram")

        return df
    
    cleaned_sparkDf = clean_data_googleMain(sparkDf)

    client.create_table(bigquery.Table(cleanGoogleScraped_db_path), exists_ok = True)
    to_gbq(cleaned_sparkDf, cleanDataset, cleanGoogleMainScraped_table_name)



    #################################################### googleReview
    
    cleanGoogleScraped_db_path = f"{project_id}.{cleanDataset}.{cleanGoogleReviewScraped_table_name}" # Schema + Table

    sparkDf = read_gbq(spark, rawDataset, googleReview)
    # print(sparkDf.show())
    print(sparkDf.count())

    time.sleep(30)
    ref_appid_sparkDf = read_gbq(spark, cleanDataset, cleanGoogleMainScraped_table_name)
    # print(sparkDf.show())
    print(ref_appid_sparkDf.count())

    # Code section for cleaning google review data
    def clean_data_googleReview(df):

        # Drop specific columns
        columns_to_drop = ['appVersion']
        df = df.drop(*columns_to_drop)

       # Filter only records where app_id in df is in ref_appid_sparkDf.select("app_Id").distinct()
        df = df.join(ref_appid_sparkDf.select("appId").distinct(), "appId", "inner")

        # Drop duplicates
        df = df.dropDuplicates(['reviewId'])

        # Remove specific strings from specific columns

        strings_to_remove = ["<b>"]
     
        def remove_strings(content, strings_to_remove):
            return reduce(lambda acc, s: acc.replace(s, ""), strings_to_remove, content)

        remove_strings_udf = udf(lambda x: remove_strings(x, strings_to_remove), StringType())

        df = df.withColumn("content", remove_strings_udf(col("content")))




        ### Language Detection Section

        # Use across all Spark session:               detect_language_udf = spark.udf.register("detect_language_udf", detect_language_function)
        # Use on current Dataframe and spark session: detect_language_udf = udf(detect_language_function, StringType())

        # # 1. Polyglot
        # def detect_language_polyglot(text):
        #     try:
        #         detector = Detector(text)
        #         return detector.language.code
        #     except:
        #         return "unknown"

        # # Define a UDF to apply language detection
        # detect_language_udf = spark.udf.register("detect_language_udf", detect_language_polyglot)

        # # Apply language detection to the 'content' column
        # df = df.withColumn("language_ployglot", detect_language_udf("content"))


        # 2. Langdetect & Text analytics
        def detect_language_langdetect(text):
            try:
                return detect(text)
            except:
                return "unknown"
            
        # Define a UDF to apply language detection
        detect_language_udf = spark.udf.register("detect_language_udf", detect_language_langdetect)

        # Apply language detection to the 'content' column
        df = df.withColumn("language_langdetect", detect_language_udf("content"))


        # # 3. gcld3
        # def detect_language_gcld3(text):
        #     try:
        #         detector = gcld3.NNetLanguageIdentifier(min_num_bytes=0, max_num_bytes=1000)
        #         result = detector.FindLanguage(text=text)
        #         lang_detected = result.language
        #         return lang_detected
        #     except:
        #         return "unknown"

        # # Define a UDF to apply language detection
        # detect_language_udf = spark.udf.register("detect_language_udf", detect_language_gcld3)

        # # Apply language detection to the 'content' column
        # df = df.withColumn("language_gcld3", detect_language_udf("content"))

        # Filter only language_langdetect = 'en'
        df = df.filter(df['language_langdetect'] == 'en')

    ########################### Wudi to insert text#################################
    ########################### TA - Start [Wudi]#################################
        #Remove stopwords
        custom_stopwords = ["don", "should", "now", "need", "working", "without", "doge", "screen", "app.",
    "first", "cant", "completely", "won't", "make", "still", "definitions",  "i'm", "many",
    "want", "game", "don't", "even", "can't", "doesn't", "worst", "it's",
    "one", "open", "work", "get", "people", "like", "good",  "nothing", "every", "would", "words",
    "actually", "the", "and", "to", "i", "app", "a", "of", "not", "it", "this", "is", "in", "your", "you", "very", "but", "for", "are", "they", "time", "have", "no", "please", "with", "so", "that", "bad",
    "app","will","ok","u","please","great","i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your" "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",
    "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",
    "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do",
    "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
    "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
    "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
    "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
    "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
    "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

        # Create TA Pipeline
        tokenizer = Tokenizer(inputCol="content", outputCol="tokens")
        stopwords_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_tokens", stopWords=custom_stopwords)
       #hashingTF = HashingTF(inputCol=stopwords_remover.getOutputCol())
       #idf= IDF(inputCol=hashingTF.getOutputCol())

        pipeline = Pipeline(stages=[tokenizer, stopwords_remover])
        TA_pipeline = pipeline.fit(df)

        #transformed_df holds the result of applying this pipeline to original df, performing tokenization, stop word removal
        df = TA_pipeline.transform(df)
        
    ########################### TA - End #################################
        return df

    cleaned_sparkDf = clean_data_googleReview(sparkDf)

    client.create_table(bigquery.Table(cleanGoogleScraped_db_path), exists_ok = True)

    print('Table created successfully')

    to_gbq(cleaned_sparkDf, cleanDataset, cleanGoogleReviewScraped_table_name)

    print('Table sent to GBQ successfully')

"""
TODO For both Apple & Google Reviews table, need to sort by appId, user ID, comment ID, date, etc
     in ascending order then take last row [drop duplicates(subset = 'columns without developers' replies', keep = 'last')]!

TODO Since review tables are cumulative and main tables are latest (both Apple & Google), ensure that review tables only contain appIds that are in main tables!
(Actually is it even a good idea for review tables to be cumulative though? Scared it may exceed the 10GB free tier limit as it's few 100k rows each pull and we doing it daily.)
"""

