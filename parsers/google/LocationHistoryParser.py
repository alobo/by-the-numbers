import json
import pandas as pd

class LocationHistoryParser:

    def __init__(self, filename):
        with open(filename) as f:
            self.d = json.load(f)

    def parse(self):
        df = pd.DataFrame(self.d['locations'])
        df['timestampMs'] = pd.to_numeric(df.timestampMs, errors='ignore')
        df['timestampMs'] = pd.to_datetime(df['timestampMs'], unit='ms')
        df = df.sort_values(by='timestampMs').reset_index(drop=True)

        df['latitudeE7'] /= 1E7
        df['longitudeE7'] /= 1E7

        df = df.drop('activity', axis=1)

        df = df.rename(columns={'timestampMs':'datetime', 'latitudeE7': 'latitude', 'longitudeE7': 'longitude'})

        return df
