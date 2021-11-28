import datetime as dt
import pandas as pd
import csv
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def jsonToCsv(url, outputcsv):
    data = pd.read_json(url)
    data.to_csv(outputcsv)
    return 'Read JSON and written to .csv'


def csvToSql():
    try:
        dbconnect = MySQLdb.connect(
                host='94.74.116.84',
                user='root',
                passwd='password',
                db='covid19'
                )
    except:
        print('Can\'t connect.')
    
    cursor = dbconnect.cursor()
    
    with open('./data.csv') as csv_file:

        csv_data = csv.reader(csv_file)

        for row in csv_data:
            cursor.execute(
            'INSERT INTO joytest(number, docusignid, publicurl, filingtype, \
                    cityagencyname, cityagencycontactname, \
                    cityagencycontacttelephone, cityagencycontactemail, \
                    bidrfpnumber, natureofcontract, datesigned, comments, \
                    filenumber, originalfilingdate, amendmentdescription, \
                    additionalnamesrequired, signername, signertitle) ' \
                    'VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", \
                    "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")',
                    row
                    )

    dbconnect.commit()
    cursor.close()

    return 'Read .csv and written to the MySQL database'


# DAG's arguments
default_args = {
        'owner': 'test_date',
        'start_date':dt.datetime(2021, 1, 1),
        'concurrency': 1,
        'retries': 0
        }

with DAG('parsing_govt_data',
        catchup=False, # To skip any intervals we didn't run
        default_args=default_args,
        schedule_interval='* 1 * * * *', # 's m h d mo y'; set to run every minute.
        ) as dag:

    opr_json_to_csv = PythonOperator(
            task_id='json_to_csv',
            python_callable=jsonToCsv,
            op_kwargs={
                'url':'https://data.sfgov.org/resource/pv99-gzft.json',
                'outputcsv':'./data.csv'
                }
            )

    opr_csv_to_sql = PythonOperator(
            task_id='csv_to_sql',
            python_callable=csvToSql
            )

opr_json_to_csv >> opr_csv_to_sql

