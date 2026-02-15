from pyspark.sql import SparkSession
import sys
import logging
from helper_utils import load_data, fill_missing_values, anonymize_pii, write_to_table

def get_logger():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger()


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

    logger = get_logger()
    # Create the spark Session
    spark = SparkSession.builder.appName('Test App').getOrCreate()
    logging.info('Created Spark Session.')

    # Step 1: Load input dataset (CSV uploaded to DBFS)
    df = load_data(spark, file_path, logger)
    logger.info("Original schema:")
    df.printSchema()

    # Step 2: Handle missing values with defaults
    df_clean = fill_missing_values(df, logger)

    # Step 3: Anonymize PII columns
    # Use SHA2 hashing to anonymize while keeping uniqueness
    df_anonymized = anonymize_pii(df_clean, logger)

    # Step 4: Add clustering column for handling hourly overwrites and save to Unity Catalog table
    write_to_table(df_anonymized, date_hour, logger)