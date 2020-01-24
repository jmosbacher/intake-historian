import sys
import requests
import getpass
import time

class HistorianAuth:
    @staticmethod
    def prompt_user():
        user = input("SC User: ")
        # if sys.stdin.isatty():
        #     user = input("User: ")
        # else:
        #     print("User: ")
        #     for line in sys.stdin:
        #         if line:
        #             user = line.rstrip()
        #     # user = sys.stdin.readline().rstrip()
        return user

    @staticmethod
    def prompt_passwd():
        pwd = getpass.getpass('Password: ')
        # if sys.stdin.isatty():
        #     pwd = getpass.getpass('Password: ')
        # else:
        #     print("Password: ")
        #     for line in sys.stdin:
        #         user = line.rstrip()
            # pwd = sys.stdin.readline().rstrip()
        return pwd

    def __init__(self, url, user=None, password=None, token=None):
        self.url = url
        self.user = user
        self.password = password
        self.token = token

    def _validate_auth(self):
        if self.token is None:
            self._authenticate()

    def _authenticate(self):
        if self.user is None:
            self.user = self.prompt_user()
        if self.password is None:
            self.password = self.prompt_passwd()
        login = {
            "username": self.user,
            "password": self.password
        }
        try:
            r = requests.post(self.url,data=login,verify=False) 
            r.raise_for_status()   
        except requests.exceptions.HTTPError:
            print ('\nError : '+str(r.json()['Message']))
        self.token = r.json()['token']

    def headers(self):
        self._validate_auth()
        return {"Authorization": self.token}

    def __call__(self):
        return self.headers()

class HistorianQuery:

    @staticmethod
    def parse_datetime(dt):
        return str(dt)

    def __init__(self, name, start=time.time(), end=time.time()+5, kind="lab",interval=300):
        self.query = {
            'name': name,
            'QueryType': kind,
            'StartDateUnix': self.parse_datetime(start),
            'EndDateUnix': self.parse_datetime(end),
            'Interval': str(interval),
        }

    def __call__(self):
        return self.query

class HistorianApi:
    def __init__(self, url, auth, params={}):
        self.url = url
        self.params = params
        self.auth = HistorianAuth(**auth)

    def read(self):
        try:
            r = requests.get(self.url, params=self.params, headers=self.auth(), verify=False)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print ('\nError. Status code ' + str(e.response.status_code) + ".")
            sys.exit()
        return r.json()