# ProjectTest

To test this project you should install and run Spark, HDFS, and Hive, then you should create an external table hive, for exemple:
CREATE EXTERNAL TABLE external_table_test
(
  id INT,
  name STRING,
  age INT
)
STORED AS PARQUET
LOCATION '/tmp/external_table_test';

# Project code and steps:
## Design and code a simple ETL “Extract – Transform – Load” pipeline that ingests 1 or multiple CSV files into a Database :
I developed the script python read_csv.py in src folder and the script test_read_csv.py in test folder for unit test. The commande to run python read_csv.py : spark-submit read_csv.py, The commande to run the unit test test_read_csv.py : python3 -m unittest test.test_read_csv

## Design and code simple REST API that exposes the recently ingested data:
    • Description: Returns the first 10 lines from Database
    • Request: Get /read/first-chunck
    • Response: 200 OK
    • Response Body: JSON
    • Response Body Description: A list of 10 JSON objects
I developed the script python rest_api.py in src folder and the script test_rest_api.py in test folder for unit test. The commande to run python rest_api.py : python3 rest_api.py, To get results you should run the commande : curl -X GET http://127.0.0.1:5000/read/first-chunk, The commande to run the unit test test_rest_api.py : python3 -m unittest test.test_rest_api.py

In src folder you found the configuration file : config.ini


