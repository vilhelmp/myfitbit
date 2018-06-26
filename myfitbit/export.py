
import os
import json
import logging
from datetime import date, time, timedelta

log = logging.getLogger(__name__)

class FitbitExport(object):
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

    def sync_sleep(self):
        month = 2015 * 12
        while 1:
            date_start = date(month // 12, month % 12 + 1, 1)
            month += 1
            date_end =   date(month // 12, month % 12 + 1, 1)

            if date_start > date.today():
                break

            partial = date_end > date.today()
            partial_filename = self.filename('sleep', 'sleep.{:04d}.{:02d}.partial.json'.format(
                date_start.year,
                date_start.month,
            ))
            filename = self.filename('sleep', 'sleep.{:04d}.{:02}.json'.format(
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
            sleep = self.client.get_sleep_range(
                date_start,
                date_end - timedelta(days=1)
            )
            self.write(filename, sleep)

    def heartrate_intraday_filenames(self):
        start = date(2017, 1, 1)
        days = 0
        while 1:
            d = start + timedelta(days=days)
            days += 1
            if d == date.today():
                return

            filename = self.filename(
                'heartrate_intraday',
                '{:04d}'.format(d.year),
                'heartrate_intraday.{:04d}.{:02d}.{:02d}.json'.format(
                    d.year,
                    d.month,
                    d.day
            ))
            yield d, filename

    def sync_heartrate_intraday(self):
        for d, filename in self.heartrate_intraday_filenames():
            if os.path.isfile(filename):
                log.info('Cached: %s', filename)
                continue

            log.info('Downloading: %s', filename)
            hr = self.client.get_heartrate_intraday(d)
            self.write(filename, hr)

    def get_sleep(self):
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
        def compress(data):
            minutes = [None] * 24 * 60
            for o in data:
                h, m, s = map(int, o['time'].split(':'))
                i = h * 60 + m
                minutes[i] = o['value']
            return minutes

        heartrate = []
        for d, filename in self.heartrate_intraday_filenames():
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

    def steps_intraday_filenames(self):
        start = date(2017, 1, 1)
        days = 0
        while 1:
            d = start + timedelta(days=days)
            days += 1
            if d == date.today():
                return

            filename = self.filename(
                'steps_intraday',
                '{:04d}'.format(d.year),
                'steps_intraday.{:04d}.{:02d}.{:02d}.json'.format(
                    d.year,
                    d.month,
                    d.day
            ))
            yield d, filename

    def get_steps_intraday(self):
        def compress(data):
            minutes = [None] * 24 * 60
            for o in data:
                h, m, s = map(int, o['time'].split(':'))
                i = h * 60 + m
                minutes[i] = o['value']
            return minutes

        steps = []
        for d, filename in self.steps_intraday_filenames():
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
        for d, filename in self.steps_intraday_filenames():
            if os.path.isfile(filename):
                log.info('Cached: %s', filename)
                continue

            log.info('Downloading: %s', filename)
            hr = self.client.get_steps_intraday(d)
            self.write(filename, hr)

    def distance_intraday_filenames(self):
        start = date(2017, 1, 1)
        days = 0
        while 1:
            d = start + timedelta(days=days)
            days += 1
            if d == date.today():
                return

            filename = self.filename(
                'distance_intraday',
                '{:04d}'.format(d.year),
                'distance_intraday.{:04d}.{:02d}.{:02d}.json'.format(
                    d.year,
                    d.month,
                    d.day
            ))
            yield d, filename

    def get_distance_intraday(self):
        def compress(data):
            minutes = [None] * 24 * 60
            for o in data:
                h, m, s = map(int, o['time'].split(':'))
                i = h * 60 + m
                minutes[i] = o['value']
            return minutes

        distance = []
        for d, filename in self.distance_intraday_filenames():
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
        for d, filename in self.distance_intraday_filenames():
            if os.path.isfile(filename):
                log.info('Cached: %s', filename)
                continue

            log.info('Downloading: %s', filename)
            hr = self.client.get_distance_intraday(d)
            self.write(filename, hr)

    def elevation_intraday_filenames(self):
        start = date(2017, 1, 1)
        days = 0
        while 1:
            d = start + timedelta(days=days)
            days += 1
            if d == date.today():
                return

            filename = self.filename(
                'elevation_intraday',
                '{:04d}'.format(d.year),
                'elevation_intraday.{:04d}.{:02d}.{:02d}.json'.format(
                    d.year,
                    d.month,
                    d.day
            ))
            yield d, filename

    def get_elevation_intraday(self):
        def compress(data):
            minutes = [None] * 24 * 60
            for o in data:
                h, m, s = map(int, o['time'].split(':'))
                i = h * 60 + m
                minutes[i] = o['value']
            return minutes

        elevation = []
        for d, filename in self.elevation_intraday_filenames():
            if not os.path.isfile(filename):
                continue
            data = json.load(open(filename))
            if not data:
                continue
            elevation.append({
                'date': d.isoformat(),
                'minutes': compress(data),
            })
        return elevation

    def sync_elevation_intraday(self):
        for d, filename in self.elevation_intraday_filenames():
            if os.path.isfile(filename):
                log.info('Cached: %s', filename)
                continue

            log.info('Downloading: %s', filename)
            hr = self.client.get_elevation_intraday(d)
            self.write(filename, hr)

    def sync_weight(self):
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
