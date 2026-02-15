from pyspark.sql.functions import col, when, lit, sha2, concat_ws, trim

def load_data(spark, file_path, logger):
    """
    Load data from CSV file into a Spark DataFrame with predefined schema.
    Args:
        spark: SparkSession object
        file_path: Path to the input CSV file in DBFS
        logger: Logger object for logging information
    Returns:
        Spark DataFrame containing the loaded data
    """
    df_table = spark.sql('select * from customer_churn_data limit 1').drop('date_hour')
    df = spark.read.option('header', True).schema(df_table.schema).csv(file_path)
    logger.info(f'Read data from csv file at path: {file_path}')
    return df

def fill_missing_values(df, logger):
    """Fill missing values in the DataFrame with appropriate defaults.
    Args:
        df: Input Spark DataFrame
        logger: Logger object for logging information
    Returns:
        Spark DataFrame with missing values filled
    """
    # Normalize the empty strings to Nulls
    for column, dtype in df.dtypes:
        if dtype == 'string':
            df = df.withColumn(column, when(trim(col(column)) == '', lit(None)).otherwise(trim(col(column))))
    
    # Example: fill missing numerical values with 0, categorical with "Unknown"
    df_clean = df.fillna({
        "CustomerID": -1,
        "Age": -1,
        "Gender": "Unknown",
        "Tenure": 0,
        "MonthlyCharges": 0.0,
        "ContractType": "Unknown",
        "InternetService": "Unknown",
        "TotalCharges": 0.0,
        "TechSupport": "Unknown",
        "Churn": "Unknown",
    })
    logger.info('filled missing values.')
    return df_clean

def anonymize_pii(df, logger):
    """
    Anonymize PII columns using SHA2 hashing.
    Args:
        df: Input Spark DataFrame
        logger: Logger object for logging information
    Returns:
        Spark DataFrame with anonymized PII columns
    """
    df_anonymized = (
        df
        .withColumn("TechSupport", sha2(col("TechSupport"), 256))
    )
    logger.info('Anonymized PII values.')
    return df_anonymized

def write_to_table(df, date_hour, logger):
    """
    Write the transformed DataFrame to a Unity Catalog table in Delta format.
    Args:
        df: Input Spark DataFrame
        date_hour: Date and hour value to be added as a column
        logger: Logger object for logging information
    Returns:
        None
    """
    df_final = df.withColumn("date_hour", lit(date_hour))\
        .dropDuplicates()
    logger.info(f'Added date_hour column with value: {date_hour}')
    
    table_name = "customer_churn_data"
    df_final.write.format("delta").mode("overwrite")\
        .option("replaceWhere",f"date_hour = '{date_hour}'")\
        .option("mergeSchema", True)\
        .saveAsTable(f"{table_name}")

    logger.info(f"Data successfully written to Unity Catalog table: {table_name} for the date_hour value: {date_hour}")