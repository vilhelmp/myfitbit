
import os
import json
import logging
from datetime import date, time, timedelta

log = logging.getLogger(__name__)

# number of days to leave out to give you time to fully sync
# usually a Fitbit device can hold 4-7 days in memory
BUFFER_DAYS = 5

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
        until either hitting the rate limit, or todays date.
        
        That is, ranged data are synced for full months. Filenames
        generated directly in function.
        
        '''
        month = 2015 * 12
        while 1:
            date_start = date(month // 12, month % 12 + 1, 1)
            month += 1
            date_end =   date(month // 12, month % 12 + 1, 1)
            if date_start > date.today(): #if syncing coming month
                break
            
            #"date_end" = first of month, if ealier date than 
            # BUFFER_DAYS ago, it is partial. Partial files will be 
            # synced again 
            partial = date_end > (date.today() - timedelta(days=BUFFER_DAYS))
            #sync to "name"/"year"/"name.year.month"
            #always create partial_filename, check if file exists later
            partial_filename = self.filename(name,
                                            '{:04d}'.format(date_start.year), 
                                            '{}.{:04d}.{:02d}.partial.json'.format(
                                            name,
                                            date_start.year,
                                            date_start.month,
                                        ))
            
            filename = self.filename(name,
                                    '{:04d}'.format(date_start.year), 
                                    '{}.{:04d}.{:02}.json'.format(
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
        """
        Iterator object for day filenames
        which goes into intraday syncs (i.e. one whole day per file).
        """
        start = date(2015, 1, 1)
        days = 0
        while 1:
            d = start + timedelta(days=days)
            days += 1
            # if date is BUFFER_DAYS days ago. stop
            #TODO: implement partial download to get whatever is there
            if d == (date.today() - timedelta(days=BUFFER_DAYS)):
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
              
    # Ranged syncs
    def sync_sleep(self):
        '''
        Downloads sleep data from the FitBit API to the local data store.
        Syncs one month at a time.
        
        There are a wealth of information. However there are two 
        possible types of sleep data
            
            - 'classic' from old devices such as Charge, Charge HR etc
            
            - 'stages' from new devices such as Charge 2, Alta HR etc
            
        The 'type' column gives which one, and the levels column 
        gives the different data. All other columns should be the same
        for the two.
        
        '''
        self.sync_ranged_data('sleep', self.client.get_sleep_range)

    def sync_weight(self):
        '''
        Downloads weight data from the FitBit API to the local data store.
        Syncs one month at a time.
        '''
        self.sync_ranged_data('weight', self.client.get_weight_range)

    #Daily summaries
    def sync_activities(self):
        '''
        Downloads daily activities data from the FitBit API
        to the local data store. Activities are not evenly spaced
        e.g. you do not go for a run exactly the same time every day...
        
        However it does contain daily summaries with information such as 
        daily resting HR, time/calories in HR zones, calories burnt, elevation, 
        floors climbed, daily steps, Very/Fairly active minutes or
        sedentary minutes.
        '''
        for d, filename in self.day_filenames('activities'):
            if os.path.isfile(filename):
                log.info('Cached: %s', filename)
                continue

            log.info('Downloading: %s', filename)
            hr = self.client.get_activities(d)
            self.write(filename, hr)
    
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

    # Functions for the report
    # i.e. simple read of the data
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
    
    def get_sleep(self):
        '''
        Return sleep data from the local store.
        Returns: [{sleep_data}, ...]
        where `sleep_data` is the inner dict from
        https://dev.fitbit.com/build/reference/web-api/sleep/
        
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

