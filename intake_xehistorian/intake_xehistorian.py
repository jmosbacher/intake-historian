# -*- coding: utf-8 -*-

"""Main module."""
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse
from intake.source import base
from collections import defaultdict
import math
import getpass
import sys
import requests
import time
from .. import __version__
from intake_xecatalog.drivers.historian_api import HistorianQuery, HistorianAuth


class XeHistorianSource(base.DataSource):
    name = 'xesc'
    container = 'python'
    partition_access = False
    version = __version__

    def __init__(self, url, query_kwargs={}, auth_kwargs={},
                    metadata=None, chunksize=100, dry=False):
        super().__init__(metadata=metadata)
        self.dry = dry
        self.url = url
        self.query = HistorianQuery(**query_kwargs)
        auth = auth_kwargs.copy()
        authurl = auth.pop("url", urlparse.urlunsplit(urlparse.urlsplit(url)[:2]+("Login","","")))
        self.auth = HistorianAuth(authurl, **auth_kwargs)

    def _get_schema(self):
        return base.Schema(datashape=None,
                           dtype=None,
                           shape=None,
                           npartitions=1,  # consider only one partition
                           extra_metadata={})

    def _get_partition(self, i):
        return self.read()

    def read(self, user=None, password=None):
        if user is not None:
            self.auth.user = user
        if password is not None:
            self.auth.password = password
        if self.dry:
            data = {
                "url": self.url,
            }
            data.update(self.query())
            data.update(self.auth())
            return data
        try:
            r = requests.get(self.url, params=self.query(), headers=self.auth(), verify=False)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print ('\nError. Status code ' + str(e.response.status_code) + ".")
            sys.exit()
        return r.json()


class XeHistorianDfSource(XeHistorianSource):
    name = 'xescdf'
    container = 'dataframe'

    def read(self, user=None, password=None):
        import pandas as pd
        data = super().read(user, password)
        df = pd.DataFrame(data)
        df.insert(0, "name", self.query()["name"]) 
        df.insert(1,"datetime", pd.to_datetime(df["timestampseconds"], unit="s") )
        return df


class XeHistorianStreamSource(base.DataSource):
    name = 'xescstream'
    container = 'streamz'
    partition_access = False

    def __init__(self, stream_kwargs, **kwargs):
        super().__init__(**kwargs)
        self.stream = None
        self.stream_kwargs = stream_kwargs

    def _get_schema(self):
        if self.stream is None:
            from xecatalog.drivers.historian_stream import HistorianStream
            self.stream = HistorianStream(**self.stream_kwargs)
        return {'stream': str(self.stream)}

    def _get_partition(self, i):
        self._get_schema()
        return self.stream

    # def read(self):
    #     return self.read_partition(0)
    