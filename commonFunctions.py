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