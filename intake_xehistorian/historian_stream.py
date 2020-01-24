import streamz.dataframe as sdf
import streamz
import time
import tornado
from tornado import gen
import sys
import requests
import getpass
import pandas as pd
import hvplot.streamz
import hvplot.pandas
from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse
from xecatalog.drivers.historian_api import HistorianAuth

class HistorianStream(sdf.DataFrame):
    """ A streaming dataframe of SC data

    Parameters
    ----------
    url:
    name: 
    freq: timedelta
        The time interval between records
    interval: timedelta
        The time interval between new dataframes, should be significantly
        larger than freq
    Example
    -------
    >>> source = HistorianStream(freq=1) 
    """
    
    def __init__(self, parameters, url, auth={}, frequency=100,
                 dask=False, start=False, timeout=2):
        if dask:
            from streamz.dask import DaskStream
            source = DaskStream()
            self.loop = source.loop
        else:
            source = streamz.Source(asynchronous=False)
            self.loop = IOLoop.current()
        self.source = source
        
        self.url = url

        self.parameters = [(param, "") if isinstance(param, str) else (param[0], param[1]) for param in parameters]
        self.frequency = frequency
        
        self.continue_ = [True]
        self.timeout = timeout
        auth_kwargs = auth.copy()
        auth_url = auth.pop("url", urlparse.urlunsplit(urlparse.urlsplit(url)[:2]+("Login","","")))
        self.auth = HistorianAuth(auth_url, **auth)
        example = self.make_df(tuple([(time.time(), name, float("nan"), unit) for name, unit in self.parameters]))
  
        stream = self.source.unique().map(self.make_df)
        super(sdf.DataFrame, self).__init__(stream, example=example)
        self.http_client = AsyncHTTPClient()
        if start:
            self.start()

    def start(self):
        self.auth()
        self.loop.add_callback(self._read_value_cb())
        
    @staticmethod
    def make_df(datas):
        data = [{"name":name, 'timestampseconds': ts, 'value': value, "unit":unit} for ts, name, value, unit in datas]
        return pd.DataFrame(data)
    
    
    def _read_value_cb(self):
        @gen.coroutine
        def cb():
            while self.continue_[0]:
                yield gen.sleep(self.frequency)
                datas = []
                for name, unit in self.parameters:
                    try:
                        resp = yield self.http_client.fetch(self.url+"?name="+name,
                                                            headers=self.auth(),validate_cert=False,
                                                               request_timeout=self.timeout,)
                        data = tornado.escape.json_decode(resp.body)[0]
        #                 print(data)
                        data = (data["timestampseconds"], name, data['value'], unit)
                    except Exception as e:
                        # print(e)
                        data = (time.time(), name, float("nan"), "")
                    datas.append(data)
                yield self.source.emit(tuple(datas))
        return cb
    
    def __del__(self):
        self.stop()

    def stop(self):
        self.continue_[0] = False

    