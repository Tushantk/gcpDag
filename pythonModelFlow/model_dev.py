import datetime
import pandas as pd
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

BUCKET_NAME = 'heading-234419-sklearn_poc'

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

        train_features = sql_data.drop('income_level', axis=1).values.tolist()

        # Create our training labels list, convert the Dataframe to a lists of lists

        train_labels = (sql_data['income_level'] == ' >50K').values.tolist()

        classifier = RandomForestClassifier()

        # Transform the features and fit them to the classifier
        classifier.fit(train_features, train_labels)

        # Create the overall model as a single pipeline
        pipeline = Pipeline([
            ('classifier', classifier)
        ])
        
        model_rf = 'model_rf.joblib'
        joblib.dump(pipeline, model_rf)

        # Upload the model to GCS
        bucket = storage.Client().bucket(BUCKET_NAME)
        blob = bucket.blob('{}/{}'.format(
            datetime.datetime.now().strftime('census_%Y%m%d_%H%M%S'),
            model_rf))
        blob.upload_from_filename(model_rf)

        print(model_rf)

finally:
    connection.close()
