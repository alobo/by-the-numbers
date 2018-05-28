import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from pipelines.SQLPipeline import SQLPipeline
from parsers.netflix.ActivityParser import NetflixActivityParser
from parsers.hbonow.ActivityParser import HBONowActivityParser

class MediaPipeline(SQLPipeline):

    DATA_SOURCE_NETFLIX = 'data/netflix/NetflixData.htm'
    DATA_SOURCE_HBONOW = 'data/hbonow/history.csv'

    DATA_SOURCE_RUNTIMES_TV = 'data/netflix/runtimes_tv.csv'
    DATA_SOURCE_RUNTIMES_MOVIE = 'data/netflix/runtimes_movies.csv'

    VIEW_DEFINITION = 'views/media.sql'

    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__()

    def extract(self):
        self.logger.info('Extract')
        assert(Path(MediaPipeline.DATA_SOURCE_NETFLIX).is_file())
        assert(Path(MediaPipeline.DATA_SOURCE_HBONOW).is_file())
        assert(Path(MediaPipeline.DATA_SOURCE_RUNTIMES_TV).is_file())
        assert(Path(MediaPipeline.DATA_SOURCE_RUNTIMES_MOVIE).is_file())

    def transform(self):
        self.logger.info('Processing Netflix')
        ap_nfx = NetflixActivityParser('data/netflix/NetflixData.htm')
        nfx = pd.DataFrame(ap_nfx.parse())

        self.logger.info('Processing HBONow')
        ap_hbo = HBONowActivityParser('data/hbonow/history.csv')
        df = ap_hbo.parse()

        # Concatenate all data
        media = nfx.append(df)
        media['date'] = pd.to_datetime(media['date'])

        # Add runtime data
        runtimes_tv = pd.read_csv('data/netflix/runtimes_tv.csv')
        runtimes_movies = pd.read_csv('data/netflix/runtimes_movies.csv')

        # Merge runtime data and cleanup columns
        media = pd.merge(media, runtimes_movies, on='title', how='outer')
        media = pd.merge(media, runtimes_tv, on='series', how='outer')
        media['runtime'] = media['runtime_x'].fillna(media['runtime_y'])
        media['runtime'] /= 60
        media = media.drop(['runtime_x', 'runtime_y'], axis = 1)
        media.head()

        self.media = media

    def load(self):
        self.logger.info('Loading Media')
        engine = create_engine(self.secrets['mysql']['connector'])
        with engine.connect() as conn, conn.begin():
            self.media.to_sql(name='media', con=engine, if_exists = 'replace', index=False)
            SQLPipeline.executeSQL(engine, self.VIEW_DEFINITION)
