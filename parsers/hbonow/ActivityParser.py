import pandas as pd

class HBONowActivityParser:

    def __init__(self, filename):
        with open(filename) as f:
            self.df = pd.read_csv(filename)
            self.df = self.df[['dtVisit', 'sUrl', 'sTitle']]
            assert(all(self.df['sUrl'].apply(lambda x: 'hbonow' in x)))

    def parse(self):
        self.df['hbo_id'] = self.df['sUrl'].apply(lambda x: x.split('episode:')[-1])
        self.df['movie'] = self.df['sUrl'].apply(lambda x: 'feature' in x)
        self.df = self.df.rename(columns={'dtVisit': 'date',
                                  'sTitle': 'series'})
        self.df = self.df.drop_duplicates('hbo_id').reset_index(drop=True)
        return self.df[['date', 'series', 'hbo_id', 'movie']]
