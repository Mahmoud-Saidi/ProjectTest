from pyhive import hive
from flask import Flask, jsonify
import configparser
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_config():
    logger.info("Reading configuration from config.ini")
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['HiveConnection']

def get_hive_connection():
    logger.info("Read configuration values")
    paths_config = read_config()

    logger.info("Get the host and port of your HiveServer2 instance")
    hive_host = paths_config['hive_host']
    hive_port = paths_config['hive_port']

    logger.info("Get the database name")
    database = paths_config['database']

    logger.info("Establish a connection to HiveServer2")
    connection = hive.Connection(host=hive_host, port=hive_port, database=database)

    return connection

@app.route('/read/first-chunk')
def query_hive():
    try:
        
        logger.info("Get a connection to HiveServer2")
        connection = get_hive_connection()

        logger.info("Create a cursor")
        cursor = connection.cursor()

        logger.info("Execute a sample query")
        query = 'SELECT * FROM external_table_test LIMIT 10'
        cursor.execute(query)

        logger.info("Fetch results")
        results = cursor.fetchall()

        logger.info("Close the cursor and connection")
        cursor.close()
        connection.close()

        logger.info("Return the results as JSON")
        return jsonify({'results': results, 'Description': "A list of 10 JSON objects"})

    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True)
