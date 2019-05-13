#!/bin/bash
#bashfiledep.sh

echo "Moving Input Data"
gsutil cp gs://heading-234419-sklearn_poc/Data_clean.py /home/airflow/gcs/data/
gsutil cp gs://heading-234419-sklearn_poc/model_dev.py /home/airflow/gcs/data/
gsutil cp gs://heading-234419-sklearn_poc/model_deploy.py /home/airflow/gcs/data/

echo "Download Dependencies"

pip install --upgrade pip --user

echo "Initiated scikit-learn"

pip install -U scikit-learn==0.20.3 --user

sudo pip install PyMySQL
sudo pip install mysql-connector
sudo apt-get install mysql-client
	
	
	

