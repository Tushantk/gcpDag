import datetime
import pandas as pd
import numpy as np
import pymysql
import pymysql.cursors
from os import getenv
import sqlalchemy
from google.cloud import storage
from sklearn.externals import joblib
from google.cloud import storage
from sklearn.preprocessing import LabelEncoder
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from sklearn.feature_selection import SelectKBest
from sklearn.pipeline import FeatureUnion
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelBinarizer
import googleapiclient.discovery
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
from sqlalchemy import create_engine


# TODO(developer): specify SQL connection details
CONNECTION_NAME = getenv(
  'INSTANCE_CONNECTION_NAME',
  'modern-heading-234419:us-central1:sklearndata-1')
DB_USER = getenv('MYSQL_USER', 'root')
DB_PASSWORD = getenv('MYSQL_PASSWORD', 'root')
DB_NAME = getenv('MYSQL_DATABASE', 'sklearn_data')
 
mysql_config = {
  'host': '35.184.7.191',
  'user': DB_USER,
  'password': DB_PASSWORD,
  'db': DB_NAME,
  'charset': 'utf8mb4',
  'cursorclass': pymysql.cursors.DictCursor,
  'autocommit': True
}

database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(DB_USER, DB_PASSWORD, 
                                                      '35.184.7.191', DB_NAME))
 

connection = pymysql.connect(**mysql_config)
connection1 = database_connection.connect()

try:
    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT * FROM train_data"
        cursor.execute(sql)
        sql_data = pd.DataFrame(cursor.fetchmany(50))

        
        test_features = sql_data.drop('income_level', axis=1).as_matrix().tolist()
        test_labels = (sql_data['income_level'] == ' >50K.').as_matrix().tolist()

        PROJECT_ID = 'modern-heading-234419'
        VERSION_NAME = 'version_new4'
        MODEL_NAME = 'version4'

        service = googleapiclient.discovery.build('ml', 'v1')
        name = 'projects/{}/models/{}'.format(PROJECT_ID, MODEL_NAME)
        name += '/versions/{}'.format(VERSION_NAME)

        first_half = test_features[:int(len(test_features))]


        complete_results = []
        for data in [first_half]:
            responses = service.projects().predict(
                name=name,
                body={'instances': data}
            ).execute()

            if 'error' in responses:
                (responses['error'])
            else:
                complete_results.extend(responses['predictions'])


        predictions = pd.DataFrame({'predict_class':complete_results})
        dataset_predict = pd.concat([sql_data, predictions], axis=1)

        dataset_predict.to_sql(con=connection1, name='predict_data', if_exists='replace', index=False)

        
        print(dataset_predict.predict_class.unique())

finally:
    connection.close()
