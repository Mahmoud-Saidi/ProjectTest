from pyspark.sql import SparkSession
import configparser
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_sparksession():
    logger.info("Creating Spark session")
    return SparkSession.builder \
        .appName("test") \
        .enableHiveSupport() \
        .getOrCreate()

def read_csv(sparksession, path_csv):
    logger.info(f"Reading CSV file from {path_csv}")
    return sparksession.read.csv(path_csv, header=True, inferSchema=True)

def write_parquet(dataframe, path_parquet, mode):
    logger.info(f"Writing Parquet file to {path_parquet} with mode {mode}")
    dataframe.write.parquet(path_parquet, mode=mode)

def read_config():
    logger.info("Reading configuration from config.ini")
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['Paths']

if __name__ == '__main__':
    try:
        # Read configuration values
        paths_config = read_config()
        csv_path = paths_config['csv_path']
        parquet_path = paths_config['parquet_path']

        # Create a Spark session
        spark = get_sparksession()

        # Read csv files
        input_df = read_csv(spark, csv_path)

        # Write dataframe to parquet
        write_parquet(input_df, parquet_path, "append")

        logger.info("Script execution completed successfully")

    except Exception as e:
        logger.error(f"Error during script execution: {str(e)}")
