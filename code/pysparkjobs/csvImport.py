"""
BEFORE RUNNING:
---------------
1. If not already done, enable the Cloud SQL Administration API
   and check the quota for your project at
   https://console.developers.google.com/apis/api/sqladmin
2. This sample uses Application Default Credentials for authentication.
   If not already done, install the gcloud CLI from
   https://cloud.google.com/sdk and run
   `gcloud beta auth application-default login`.
   For more information, see
   https://developers.google.com/identity/protocols/application-default-credentials
3. Install the Python client library for Google APIs by running
   `pip install --upgrade google-api-python-client`
"""

from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

from time import sleep

credentials = GoogleCredentials.get_application_default()

service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

# Project ID of the project that contains the instance.
project = 'able-cogency-234306'  # TODO: Update placeholder value.

# Cloud SQL instance ID. This does not include the project ID.
instance = 'testddd'  # TODO: Update placeholder value.

instances_import_request_body = {
  "importContext": {
    "database": "recommendation_spark",
    "fileType": "CSV",
    "uri": "gs://able-cogency-234306/tmp/accommodation.csv",
    "csvImportOptions": {
      "table": "Accommodation",
      "columns": [
        "id",
        "title",
        "location",
        "price",
        "rooms",
        "rating",
        "type"
      ]
    },
    "kind": "sql#importContext"
  }
}

request = service.instances().import_(project=project, instance=instance, body=instances_import_request_body)
response = request.execute()
pprint(response)

sleep(20)

instances_import_request_body2 = {
  "importContext": {
    "database": "recommendation_spark",
    "fileType": "CSV",
    "uri": "gs://able-cogency-234306/tmp/rating.csv",
    "csvImportOptions": {
      "table": "Rating",
      "columns": [
        "userId",
        "accoId",
        "rating"
      ]
    },
    "kind": "sql#importContext"
  }
}
request2 = service.instances().import_(project=project, instance=instance, body=instances_import_request_body2)
response = request2.execute()
# TODO: Change code below to process the `response` dict:
pprint(response)