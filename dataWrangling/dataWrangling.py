from functools import reduce
from common import read_gbq, to_gbq
import ast
import time
from google.cloud import bigquery
from dataSources.deleteRowsAppleGoogle import rawDataset, googleScraped_table_name
from pyspark.sql.functions import col, regexp_replace, split, expr, udf, when
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

def appleDataWrangling(spark, project_id, client):
    return

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

    # Code section for cleaning googleMain data
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


        # 2. Langdetect
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
        hashingTF = HashingTF(inputCol=stopwords_remover.getOutputCol(), outputCol="rawFeatures", numFeatures=10000)
        idf= IDF(inputCol=hashingTF.getOutputCol(), outputCol="features")

        pipeline = Pipeline(stages=[tokenizer, stopwords_remover, hashingTF, idf])
        TA_pipeline = pipeline.fit(df)

        #transformed_data holds the result of applying this pipeline to original DataFrame english_googleReview_sparkDF, performing tokenization, stop word removal
        transformed_data = TA_pipeline.transform(df)
        
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

