import requests
from config.api_config import API_CONFIG
import inspect
from functools import wraps
import pickle
from util import Spinner
import json
import pandas as pd


def soap_request(func):
    signature = inspect.signature(func)

    @wraps(func)
    def call(*args, **kwargs):
        kwargs = signature.bind(*args, **kwargs).arguments
        kwargs.pop('self', None)

        tickers = kwargs['tickers']
        item = kwargs.get('item', None) or kwargs.get("items")
        if isinstance(tickers, list):
            if func.__name__ == "get_time_series_data" and len(tickers) > 20:
                raise ValueError("get_time_series_data의 tickers는 최대 20개까지 요청가능합니다.")
            tickers = ",".join(tickers[:10])

        msg = '[DataStreamAPI] : requesting {} {}'.format(tickers[:10] + "..." if len(tickers) > 20 else tickers, item)
        with Spinner(msg):
            try:
                data = pickle.dumps(((API_CONFIG['DSWS_ID'], API_CONFIG['DSWS_PW']), func.__name__, kwargs))
                return pickle.loads(requests.post(API_CONFIG['DATASTREAM_SOAP_ADDRESS'], data=data).content)
            except:
                data = pickle.dumps(((API_CONFIG['DSWS_ID'], API_CONFIG['DSWS_PW']), func.__name__, kwargs))
                return pickle.loads(requests.post(API_CONFIG['DATASTREAM_SUB_SOAP_ADDRESS'], data=data).content)

    return call


class SoapDataStreamAPI(object):

    @soap_request
    def get_time_series_data(self, tickers, item='', date_from='1986-12-31', date_to='9999-12-31', frequency='M',
                             day_lag=0, convert_to_rate=False, daily_to_monthly=False, daily_to_monthly_method='last',
                             ffill=True):
        pass

    @soap_request
    def get_static_data(self, tickers, items: list or str, is_constituents, date='9999-12-31'):
        pass


if __name__ == '__main__':
    dsa = SoapDataStreamAPI()
    tickers = ['S&PCOMP']
    df = dsa.get_time_series_data(tickers, "PI", frequency="D")
    df = dsa.get_static_data(tickers, "P", False)
    print(df)
