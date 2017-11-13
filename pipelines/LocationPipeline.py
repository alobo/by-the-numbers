import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from pipelines.Pipeline import Pipeline
from parsers.google.LocationHistoryParser import LocationHistoryParser

class LocationPipeline(Pipeline):

    DATA_SOURCE_LOCATION = 'data/google/Location History/Location History.json'

    logger = logging.getLogger(__name__)

    def __init__(self):
        Pipeline.__init__(self)

    def extract(self):
        self.logger.info('Extract')
        assert(Path(LocationPipeline.DATA_SOURCE_LOCATION).is_file())

    def transform(self):
        self.logger.info('Transforming Location History')
        lh_parser = LocationHistoryParser(LocationPipeline.DATA_SOURCE_LOCATION)
        self.location = lh_parser.parse()

    def load(self):
        self.logger.info('Loading Location History')
        engine = create_engine(self.secrets['mysql']['connector'])
        with engine.connect() as conn, conn.begin():
            self.location.to_sql(name='location', con=engine, if_exists = 'replace', index=False)
