import argparse
import configparser
import getpass
import logging
import json

import requests
from requests.exceptions import HTTPError

from . import Fitbit, FitbitAuth
from .export import FitbitExport

logging.basicConfig(level=logging.DEBUG)

def main():
    config = configparser.ConfigParser()
    config.read('myfitbit.ini')

    fa = FitbitAuth(
        client_id=config['fitbit_auth']['client_id'],
        client_secret=config['fitbit_auth']['client_secret'],
    )
    fa.ensure_access_token()

    try:
        f = Fitbit(access_token=fa.access_token['access_token'])
        print(json.dumps(f.profile, indent=2))
    except requests.exceptions.HTTPError as e:
        print(e.response.status_code)
        if e.response.status_code == 429:
            print(e.response.headers)
            return
        raise

    export = FitbitExport('.', f)
    try:
        # Montly summaries per file
        export.sync_weight()
        export.sync_sleep()
        # Daily summaries per file
        export.sync_activities()
        # Daily (intraday) data per file
        export.sync_heartrate_intraday()
        export.sync_steps_intraday()
        export.sync_distance_intraday()

    except HTTPError as e:
        status_code = e.response.status_code
        #    if status_code == '429':
        print( "HTTP error status code: {0}".format(status_code) )
        
if __name__ == '__main__':
    main()
