from functools import reduce
from common import read_gbq, to_gbq
from google.cloud import bigquery
from dataSources.deleteRowsAppleGoogle import rawDataset, googleScraped_table_name
from pyspark.sql.functions import col, regexp_replace, split, expr, udf
from pyspark.sql.types import ArrayType, StringType
# from googletrans import Translator
import ast


# Hard-coded variables
cleanDataset = "cleanData" # Schema
cleanGoogleMainScraped_table_name = 'cleanGoogleMain' # Table
cleanGoogleReviewScraped_table_name = 'cleanGoogleReview' # Table
cleanAppleMainScraped_table_name = 'cleanAppleMain' # Table
cleanAppleReviewScraped_table_name = 'cleanAppleReview' # Table

trainTestdata = "trainTestData" # Schema
googleMain = "googleMain" # Table
googleReview = "googleReview" # Table
appleMain = "appleMain" # Table
appleReview = "appleReview" # Table

# TODO Follow this template when scripting!!
def dataWrangling(spark, project_id, client):

    # googleMain
    
    cleanGoogleScraped_db_path = f"{project_id}.{cleanDataset}.{cleanGoogleMainScraped_table_name}" # Schema + Table

    sparkDf = read_gbq(spark, trainTestdata, googleMain)
    # print(sparkDf.show())
    print(sparkDf.count())

    # Code section for cleaning googleMain data
    def clean_data_googleMain(df):

        # Drop specific columns
        columns_to_drop = ['descriptionHTML', 'sale', 'saleTime', 'originalPrice', 'saleText', 'developerId', 'containsAds', 'updated']
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

            def remove_strings_from_df(df, column, strings):
                for string in strings:
                    df = df.withColumn(column, remove_string_from_column(column, string))
                return df

            return reduce(lambda acc, column: remove_strings_from_df(acc, column, strings_to_remove[column]), strings_to_remove, df)

        df = remove_strings_from_columns(df, strings_to_remove)





        # Convert categories list dictionary into list
        # Define the extract_names_udf function as a UDF

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
        

        
        # Drop records where 'ratings' column has a value of 0
        df = df.filter(col("ratings") != 0)

        # Drop records where 'minInstalls' column is None
        df = df.filter(col("ratings") != 0)

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



    # googleReview
    
    cleanGoogleScraped_db_path = f"{project_id}.{cleanDataset}.{cleanGoogleReviewScraped_table_name}" # Schema + Table

    sparkDf = read_gbq(spark, trainTestdata, googleReview)
    # print(sparkDf.show())
    print(sparkDf.count())

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

        ### Translate function takes too long to run (>6hrs)
        # def translate_text(text):
        #     try:
        #         translator = Translator()
        #         translation = translator.translate(text, dest='en')
        #         return translation.text
        #     except Exception as e:
        #         return str(e)
        
        # # Register the translation function as a UDF
        # translate_udf = udf(translate_text)

        # # Apply translation to the DataFrame
        # df = df.withColumn("translated_text", translate_udf("content"))

        return df
    
    cleaned_sparkDf = clean_data_googleReview(sparkDf)

    client.create_table(bigquery.Table(cleanGoogleScraped_db_path), exists_ok = True)
    to_gbq(cleaned_sparkDf, cleanDataset, cleanGoogleReviewScraped_table_name)


"""
TODO For both Apple & Google Reviews table, need to sort by appId, user ID, comment ID, date, etc
     in ascending order then take last row [drop duplicates(subset = 'columns without developers' replies', keep = 'last')]!

TODO Since review tables are cumulative and main tables are latest (both Apple & Google), ensure that review tables only contain appIds that are in main tables!
(Actually is it even a good idea for review tables to be cumulative though? Scared it may exceed the 10GB free tier limit as it's few 100k rows each pull and we doing it daily.)
"""

