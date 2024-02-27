from google.cloud import bigquery

def to_gbq(pandasDf, client, dataSet_tableName, mergeType ='WRITE_TRUNCATE'):
    df = pandasDf.copy()
    job_config = bigquery.LoadJobConfig(write_disposition=mergeType)
    load_job = client.load_table_from_dataframe(
        df,
        dataSet_tableName,
        job_config=job_config
    )
    return load_job

def to_gbq_parquet(parquet_file_path, client, dataSet_tableName, mergeType='WRITE_TRUNCATE'):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=mergeType,
    )
    load_job = client.load_table_from_uri(
        parquet_file_path,
        dataSet_tableName,
        job_config=job_config
    )
    return load_job

def split_df(df, noOfSlices = 1, subDf = 1):
    # Assuming df is your DataFrame
    num_parts = noOfSlices

    # Calculate the number of rows in each part
    num_rows = len(df)
    rows_per_part = num_rows // num_parts

    # Initialize a list to store the sub DataFrames
    sub_dfs = []

    # Split the DataFrame into parts
    for i in range(num_parts):
        start_idx = i * rows_per_part
        end_idx = start_idx + rows_per_part
        if i == num_parts - 1:  # For the last part, include the remaining rows
            end_idx = num_rows
        sub_df = df.iloc[start_idx:end_idx]
        sub_dfs.append(sub_df)

    # Select sub DataFrame
    indexOfSubDf = subDf - 1
    small_df = sub_dfs[indexOfSubDf]

    return small_df