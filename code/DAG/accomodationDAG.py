# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START composer_quickstart]
"""Example Airflow DAG that creates a Cloud Dataproc cluster, runs the Hadoop
wordcount example, and deletes the cluster.
This DAG relies on three Airflow variables
https://airflow.apache.org/concepts.html#variables
* gcp_project - Google Cloud Project to use for the Cloud Dataproc cluster.
* gce_zone - Google Compute Engine zone where Cloud Dataproc cluster should be
  created.
* gcs_bucket - Google Cloud Storage bucket to use for result of Hadoop job.
  See https://cloud.google.com/storage/docs/creating-buckets for creating a
  bucket.
"""

import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

# Path to Hadoop wordcount example available on every Dataproc cluster.
CSVIMPORTPY = (
    'gs://able-cogency-234306/tmp/csvImport.py'
)

MODELPY = (
    'gs://able-cogency-234306/tmp/train_and_apply.py'
)

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

# [START composer_quickstart_schedule]
with models.DAG(
        'composer_accomodation_model',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    # [END composer_quickstart_schedule]

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        # Give the cluster a unique name by appending the date scheduled.
        # See https://airflow.apache.org/code.html#default-variables
        cluster_name='accomodation-cluster-{{ ds_nodash }}',
        num_workers=2,
        init_actions_uris=['gs://able-cogency-234306/tmp/cloud-sql-proxy.sh'],
        service_account_scopes=['https://www.googleapis.com/auth/cloud-platform','https://www.googleapis.com/auth/sqlservice.admin'],
        metadata={'enable-cloud-sql-hive-metastore':'false','additional-cloud-sql-instances':'able-cogency-234306:us-central1:testddd'},
        region='us-central1',
        zone=models.Variable.get('gce_zone'),
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

    # Run the Hadoop wordcount example installed on the Cloud Dataproc cluster
    # master node.
    csv_import_job = dataproc_operator.DataProcPySparkOperator(
        task_id='csv_import_job',
        main=CSVIMPORTPY,
        cluster_name='accomodation-cluster-{{ ds_nodash }}',
		job_name='csv_import_job',
        region='us-central1')
	
	# Run the Hadoop wordcount example installed on the Cloud Dataproc cluster
    # master node.
    accomodation_model_job = dataproc_operator.DataProcPySparkOperator(
        task_id='accomodation_model_job',
        main=MODELPY,
        cluster_name='accomodation-cluster-{{ ds_nodash }}',
		job_name='accomodation_model_job',
        region='us-central1')

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='accomodation-cluster-{{ ds_nodash }}',
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        region='us-central1',
       trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # [START composer_quickstart_steps]
    # Define DAG dependencies.
    create_dataproc_cluster >> csv_import_job >> accomodation_model_job >> delete_dataproc_cluster
    # [END composer_quickstart_steps]

# [END composer_quickstart]