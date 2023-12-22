import unittest
from unittest.mock import patch
from src.rest_api import app, get_hive_connection
import logging

class MyAppTestCase(unittest.TestCase):
    def setUp(self):
        logging.info('Set up a test client')
        self.app = app.test_client()

    @patch('src.rest_api.get_hive_connection')
    def test_query_hive_success(self, mock_get_hive_connection):
        logging.info('Mock the get_hive_connection function to return a mock connection')
        mock_connection = mock_get_hive_connection.return_value
        mock_cursor = mock_connection.cursor.return_value

        logging.info('Mock the execute and fetchall methods')
        mock_cursor.fetchall.return_value = [['value1', 'value2'], ['value3', 'value4']]

        logging.info('Make a request to the /read/first-chunk endpoint')
        response = self.app.get('/read/first-chunk')

        logging.info('Assert that the response is successful and contains the expected data')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, {'results': [['value1', 'value2'], ['value3', 'value4']], 'Description': "A list of 10 JSON objects"})

    @patch('src.rest_api.get_hive_connection')
    def test_query_hive_failure(self, mock_get_hive_connection):
        logging.info('Mock the get_hive_connection function to raise an exception')
        mock_get_hive_connection.side_effect = Exception('Test error')

        logging.info('Make a request to the /read/first-chunk endpoint')
        response = self.app.get('/read/first-chunk')

        logging.info('Assert that the response indicates an error')
        self.assertEqual(response.status_code, 200)  # Assuming you want a 200 status for errors
        self.assertEqual(response.json, {'error': 'Test error'})

if __name__ == '__main__':
    # Configure the logging format and level
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    unittest.main()
