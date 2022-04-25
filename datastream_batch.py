from datastream_soap_api import SoapDataStreamAPI

import glob
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

from client_bigquery import GoogleBigQuery as gbq

key_path = glob.glob("./config/*.json")[0]
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials,
                         project=credentials.project_id)

ds = SoapDataStreamAPI()

schema_ds_series = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("value", "STRING"),
    bigquery.SchemaField("last_updated", "STRING")
]

schema_ds_ticker = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("field", "STRING")
]

table_id_datastream = f"innate-plexus-345505.datastream.datastream_data"

gbqClient = gbq.getClient()

sql_ticker = f"""
            SELECT  ticker, field 
            FROM    innate-plexus-345505.datastream.ticker
            """

df_tickers = gbqClient.query(sql_ticker).to_dataframe()






for ticker in df_tickers:
    # raw data 적재가 목적이므로, daily to monthly 파라미터 제외
    raw_data = ds.get_time_series_data(ticker['ticker'],
                                       ticker['field'],
                                       date_from='1986-12-31',
                                       date_to='9999-12-31',
                                       frequency='D',
                                       daily_to_monthly=False)
    df_raw_data = pd.DataFrame(raw_data)

    # 최종일 Date DB에서 불러와서 비교, get time series_data 와 최종일 비교하여 업데이트 여부 결정
    if True:
        df_raw_data = df_raw_data.set_index("id", drop=True)
        df_raw_data["date"] = pd.to_datetime(df_raw_data["date"])

        datastream_job_config = bigquery.LoadJobConfig(
            bigquery.LoadJobConfig(schema=schema_ds_series,
                                   autodetect=False,
                                   write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        )
        client.load_table_from_dataframe(df_raw_data, table_id_datastream, job_config=datastream_job_config).result()
        table = client.get_table(table_id_datastream)
