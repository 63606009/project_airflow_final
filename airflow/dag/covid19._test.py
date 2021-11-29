# For scheduling
import datetime as dt

# For function jsonToCsv
import pandas as pd

# For function csvToSql
import csv
import pymysql

# Backwards compatibility of pymysql to mysqldb
pymysql.install_as_MySQLdb()

# Importing MySQLdb now
import MySQLdb

# For Apache Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Step 2: Define functions for operators.

def jsonToCsv(url, outputcsv):
    data = pd.read_json(url)
    data.to_csv(outputcsv)
    return 'Read JSON and written to .csv'


def csvToSql():

    # Attempt connection to a database
    try:
        dbconnect = MySQLdb.connect(
                host='49.0.199.27',
                user='root',
                passwd='password',
                db='test2'
                )
    except:
        print('Can\'t connect.')

    # Define a cursor iterator object to function and to traverse the database.
    cursor = dbconnect.cursor()
    # Open and read from the .csv file
    with open('./joy.csv') as csv_file:

        # Assign the .csv data that will be iterated by the cursor.
        csv_data = csv.reader(csv_file)

        # Insert data using SQL statements and Python
        for row in csv_data:
            cursor.execute(
            'INSERT INTO covid19_test(number, new_case, new_case_excludeabroad, \
                    new_death, new_recovered, total_case, total_case_excludeabroad, \
                    total_death, total_recovered, txn_date)' \
                    'VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")',
                    row
                    )

    # Commit the changes
    dbconnect.commit()
    # Close the connection
    cursor.close()

    # Confirm completion
    return 'Read .csv and written to the MySQL database'


# DAG's arguments
default_args = {
        'owner': 'game',
        'start_date':dt.datetime(2021, 1, 1),
        }

# DAG's operators, or bones of the workflow
with DAG('parsing_govt_data',
        catchup=False, # To skip any intervals we didn't run
        default_args=default_args,
        schedule_interval='* 1 * * * *', # 's m h d mo y'; set to run every minute.
        ) as dag:

    opr_json_to_csv = PythonOperator(
            task_id='json_to_csv',
            python_callable=jsonToCsv,
            op_kwargs={
                'url':'https://covid19.ddc.moph.go.th/api/Cases/today-cases-all',
                'outputcsv':'./joy.csv'
                }
            )

    opr_csv_to_sql = PythonOperator(
            task_id='csv_to_sql',
            python_callable=csvToSql
            )

# The actual workflow
opr_json_to_csv >> opr_csv_to_sql

