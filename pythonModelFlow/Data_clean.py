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

COLUMNS = (
    'age',
    'workclass',
    'fnlwgt',
    'education',
    'education_num',
    'marital_status',
    'occupation',
    'relationship',
    'race',
    'sex',
    'capital_gain',
    'capital_loss',
    'hours_per_week',
    'native_country',
    'income_level'
)

# Categorical columns are columns that need to be turned into a numerical value to be used by scikit-learn
CATEGORICAL_COLUMNS = (
    'workclass',
    'education',
    'marital_status',
    'occupation',
    'relationship',
    'race',
    'sex',
    'native_country'
)

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
        class MultiColumnLabelEncoder:
            def __init__(self,columns = None):
                self.columns = columns # array of column names to encode

            def fit(self,X,y=None):
                return self # not relevant here

            def transform(self,X):
                '''
                Transforms columns of X specified in self.columns using
                LabelEncoder(). If no columns specified, transforms all
                columns in X.
                '''
                output = X.copy()
                if self.columns is not None:
                    for col in self.columns:
                        output[col] = LabelEncoder().fit_transform(output[col])
                else:
                    for colname,col in output.iteritems():
                        output[colname] = LabelEncoder().fit_transform(col)
                return output

            def fit_transform(self,X,y=None):
                return self.fit(X,y).transform(X)

        clean_data =MultiColumnLabelEncoder(CATEGORICAL_COLUMNS).fit_transform(sql_data)
        clean_data = clean_data.replace(0, 1)

        clean_data.to_sql(con=connection1, name='train_data', if_exists='replace', index=False)

        print(clean_data.head())

finally:
    connection.close()
