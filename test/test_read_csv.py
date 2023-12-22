import unittest
import tempfile
import shutil
import os
import logging
from src.read_csv import *

class TestReadCSV(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        logging.info(f'Set up temporary directory: {cls.temp_dir}')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.temp_dir)
        logging.info(f'Cleaned up temporary directory: {cls.temp_dir}')

    def setUp(self):
        self.spark = get_sparksession()
        logging.info('Spark session created for testing.')

    def tearDown(self):
        self.spark.stop()
        logging.info('Spark session stopped after testing.')

    def test_read_csv(self):
        csv_path = os.path.join(self.temp_dir, "test_data.csv")
        with open(csv_path, "w") as csv_file:
            csv_file.write("id,name\n1,Alice\n2,Bob\n")

        result_df = read_csv(self.spark, csv_path)
        self.assertEqual(result_df.count(), 2)
        logging.info('read_csv test passed successfully.')

    def test_write_parquet(self):
        test_data = [("Alice", 25), ("Bob", 30)]
        schema = ["name", "age"]
        test_df = self.spark.createDataFrame(test_data, schema)

        parquet_path = os.path.join(self.temp_dir, "test_parquet")

        write_parquet(test_df, parquet_path, "overwrite")
        result_df = self.spark.read.parquet(parquet_path)
        self.assertEqual(result_df.count(), 2)
        logging.info('write_parquet test passed successfully.')

if __name__ == '__main__':
    # Configure the logging format and level
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    unittest.main()
