import glob
from google.cloud import bigquery
from google.oauth2 import service_account
from config.api_config import API_CONFIG



class GoogleBigQuery(object, ):
    key_path = glob.glob("./config/*.json")[0]
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials,
                             project=credentials.project_id)

    def getClient(self):
        return self.client
