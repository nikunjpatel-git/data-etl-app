from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, sha2, concat_ws, trim
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a logger
logger = logging.getLogger()


if __name__ == '__main__':
    debug = False
    if debug:
        file_path = '/Workspace/Users/npatel17da@gmail.com/customer_churn_data.csv'
        date_hour = '2026020223'
    else:
        if len(sys.argv) > 2:
            file_path = sys.argv[1] # '/Workspace/Users/npatel17da@gmail.com/customer_churn_data.csv'
            date_hour = sys.argv[2] # '2026010123' -- yyyymmddHH
        else:
            raise Exception('Required Input Arguments missing....')

    # Create the spark Session
    spark = SparkSession.builder.appName('Test App').getOrCreate()
    logging.info('Created Spark Session.')

    # Step 1: Load input dataset (CSV uploaded to DBFS)
    df_table = spark.sql('select * from customer_churn_data limit 1').drop('date_hour')
    df = spark.read.option('header', True).schema(df_table.schema).csv(file_path)
    logging.info(f'Read data from csv file at path: {file_path}')

    logger.info("Original schema:")
    df.printSchema()

    # Step 2: Handle missing values with defaults
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

    # Step 3: Anonymize PII columns
    # Use SHA2 hashing to anonymize while keeping uniqueness
    df_anonymized = (
        df_clean
        .withColumn("TechSupport", sha2(col("TechSupport"), 256))
    )
    logger.info('Anonymized PII values.')

    # Step 4: Add clustering column for handling hourly overwrites
    df_final = df_anonymized.withColumn("date_hour", lit(date_hour))\
        .dropDuplicates()
    logger.info(f'Added date_hour column with value: {date_hour}')

    # Step 4: Save transformed dataset into Unity Catalog table
    table_name = "customer_churn_data"
    df_final.write.format("delta").mode("overwrite")\
        .option("replaceWhere",f"date_hour = '{date_hour}'")\
        .option("mergeSchema", True)\
        .saveAsTable(f"{table_name}")

    logger.info(f"Data successfully written to Unity Catalog table: {table_name} for the date_hour value: {date_hour}")