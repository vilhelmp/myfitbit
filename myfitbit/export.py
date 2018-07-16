
import os
import json
import logging
from datetime import date, time, timedelta

log = logging.getLogger(__name__)

class FitbitExport(object):
    '''
    Local data store of Fitbit json objects.
    '''
    def __init__(self, root, client=None, user_id=None):
        self.root = os.path.abspath(root)
        self.client = client
        self.user_id = user_id

    def filename(self, *args):
        u = self.client and self.client.user_id or self.user_id
        return os.path.join(self.root, u, *args)

    @staticmethod
    def write(filename, data):
        dirname = os.path.dirname(filename)
        os.makedirs(dirname, exist_ok=True)
        with open(filename, 'w') as f:
            f.write(json.dumps(data, indent=2, sort_keys=True))

    def sync_ranged_data(self, name, client_fn):
        '''
        Downloads date-range time series data from
        the FitBit API to the local data store.
        
        Starts from 2015 and moves forward one month at a time 
        until either hitting the rate limit, or todays date - 5 days.
        
        '''
        month = 2015 * 12
        while 1:
            date_start = date(month // 12, month % 12 + 1, 1)
            month += 1
            date_end =   date(month // 12, month % 12 + 1, 1)

            if date_start > (date.today()-timedelta(days=5)):
                break
            if date_end > (date.today()-timedelta(days=5)):
                date_end = (date.today()-timedelta(days=5))
            # now check if the dates are ordered wrong
            if date_start>date_end:
                break
            
            partial = date_end > (date.today()-timedelta(days=5))
            partial_filename = self.filename(name, '{}.{:04d}.{:02d}.partial.json'.format(
                name,
                date_start.year,
                date_start.month,
            ))
            filename = self.filename(name, '{}.{:04d}.{:02}.json'.format(
                name,
                date_start.year,
                date_start.month,
            ))

            if os.path.isfile(partial_filename):
                os.remove(partial_filename)

            if partial:
                filename = partial_filename
            elif os.path.isfile(filename):
                log.info('Cached: %s', filename)
                continue

            log.info('Downloading: %s', filename)
            data = client_fn(
                date_start,
                date_end - timedelta(days=1)
            )
            self.write(filename, data)

    def day_filenames(self, name):
        start = date(2015, 1, 1)
        days = 0
        while 1:
            d = start + timedelta(days=days)
            days += 1
            if d == (date.today()-timedelta(days=5)):
                return

            filename = self.filename(
                name,
                '{:04d}'.format(d.year),
                '{}.{:04d}.{:02d}.{:02d}.json'.format(
                    name,
                    d.year,
                    d.month,
                    d.day
            ))
            yield d, filename

    def month_filenames(self, name):
        month = 2015 * 12
        return "Not implemented"
        
    # Ranged syncs
    def sync_sleep(self):
        '''
        Downloads sleep data from the FitBit API to the local data store.
        Syncs one month at a time.
        '''
        self.sync_ranged_data('sleep', self.client.get_sleep_range)

    def sync_heartrate(self):
        '''
        Downloads heartrate data from the FitBit API to the local data store.
        Syncs one month at a time.
        '''
        self.sync_ranged_data('heartrate', self.client.get_heartrate_range)

    #Daily syncs
    def sync_activities(self):
        '''
        Downloads daily activities data from the FitBit API
        to the local data store. Activities are not evenly spaced
        e.g. you do not go for a run exactly the same time every day...
        '''
        for d, filename in self.day_filenames('activities'):
            if os.path.isfile(filename):
                log.info('Cached: %s', filename)
                continue

            log.info('Downloading: %s', filename)
            hr = self.client.get_activities(d)
            self.write(filename, hr)

    def get_sleep(self):
        '''
        Return sleep data from the local store.
        Returns: [{sleep_data}, ...]
        where `sleep_data` is the inner dict from
        https://dev.fitbit.com/build/reference/web-api/sleep/
        
        Syncs one day (night) at a time. There are a wealth of 
        information. However there are two possible types of sleep data
            
            - 'classic' from old devices such as Charge, Charge HR etc
            
            - 'stages' from new devices such as Charge 2, Alta HR etc
            
        The 'type' column gives which one, and the levels column 
        gives the different data. All other columns should be the same
        for the two.
        '''
        sleep = []
        for dir, dirs, files in os.walk(self.filename('sleep')):
            for file in files:
                filename = os.path.join(dir, file)
                data = json.load(open(filename))
                if not data:
                    continue
                sleep.extend(data)
        return sleep

    # Intraday syncs
    def sync_heartrate_intraday(self):
        '''
        Downloads heartrate intraday data from the FitBit API
        to the local data store. 
        '''
        for d, filename in self.day_filenames('heartrate_intraday'):
            if os.path.isfile(filename):
                log.info('Cached: %s', filename)
                continue

            log.info('Downloading: %s', filename)
            hr = self.client.get_heartrate_intraday(d)
            self.write(filename, hr)

    def get_heartrate_intraday(self):
        '''
        Return heartrate intraday data from the local store.
        Returns: [{hr_data}, ...]
        where `hr_data` is:
        {
            "date": "2016-07-08",
            "minutes": [int, ...]
        }
        minutes is an array of 1440 minutes in the day and the HR during that minute
        
        It is possible to get 1 sec resolution, but this sync gives 1 min.
        
        
        '''

        def compress(data):
            minutes = [None] * 24 * 60
            for o in data:
                h, m, s = map(int, o['time'].split(':'))
                i = h * 60 + m
                minutes[i] = o['value']
            return minutes

        heartrate = []
        for d, filename in self.day_filenames('heartrate_intraday'):
            if not os.path.isfile(filename):
                continue
            data = json.load(open(filename))
            if not data:
                continue
            heartrate.append({
                'date': d.isoformat(),
                'minutes': compress(data),
            })
        return heartrate

    def get_steps_intraday(self):
        def compress(data):
            minutes = [None] * 24 * 60
            for o in data:
                h, m, s = map(int, o['time'].split(':'))
                i = h * 60 + m
                minutes[i] = o['value']
            return minutes

        steps = []
        for d, filename in self.day_filenames('steps_intraday'):
            if not os.path.isfile(filename):
                continue
            data = json.load(open(filename))
            if not data:
                continue
            steps.append({
                'date': d.isoformat(),
                'minutes': compress(data),
            })
        return steps

    def sync_steps_intraday(self):
        """Downloads steps intraday data from the FitBit API
        to the local data store. """
        for d, filename in self.day_filenames('steps_intraday'):
            if os.path.isfile(filename):
                log.info('Cached: %s', filename)
                continue

            log.info('Downloading: %s', filename)
            hr = self.client.get_steps_intraday(d)
            self.write(filename, hr)

    def get_distance_intraday(self):
        def compress(data):
            minutes = [None] * 24 * 60
            for o in data:
                h, m, s = map(int, o['time'].split(':'))
                i = h * 60 + m
                minutes[i] = o['value']
            return minutes

        distance = []
        for d, filename in self.day_filenames('distance_intraday'):
            if not os.path.isfile(filename):
                continue
            data = json.load(open(filename))
            if not data:
                continue
            distance.append({
                'date': d.isoformat(),
                'minutes': compress(data),
            })
        return distance

    def sync_distance_intraday(self):
        """Downloads distance intraday data from the FitBit API
        to the local data store. """
        for d, filename in self.day_filenames('distance_intraday'):
            if os.path.isfile(filename):
                log.info('Cached: %s', filename)
                continue

            log.info('Downloading: %s', filename)
            hr = self.client.get_distance_intraday(d)
            self.write(filename, hr)

    # Monthly syncs
    def sync_weight(self):
        """Downloads weight data from the FitBit API
        to the local data store. 
        
        One month at a time.
        """
        month = 2015 * 12
        while 1:
            # sync start 2015, one month at a time 
            date_start = date(month // 12, month % 12 + 1, 1)
            month += 1
            date_end =   date(month // 12, month % 12 + 1, 1)

            if date_start > date.today():
                break

            partial = date_end > date.today()
            partial_filename = self.filename('weight', 'weight.{:04d}.{:02d}.partial.json'.format(
                date_start.year,
                date_start.month,
            ))
            filename = self.filename('weight', 'weight.{:04d}.{:02}.json'.format(
                date_start.year,
                date_start.month,
            ))

            if os.path.isfile(partial_filename):
                os.remove(partial_filename)

            if partial:
                filename = partial_filename
            elif os.path.isfile(filename):
                log.info('Cached: %s', filename)
                continue

            log.info('Downloading: %s', filename)
            weight = self.client.get_weight_range(
                date_start,
                date_end - timedelta(days=1)
            )
            self.write(filename, weight)
