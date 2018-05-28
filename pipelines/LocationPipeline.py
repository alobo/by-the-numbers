import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from pipelines.SQLPipeline import SQLPipeline
from parsers.google.LocationHistoryParser import LocationHistoryParser

class LocationPipeline(SQLPipeline):

    DATA_SOURCE_LOCATION = 'data/google/Location History/Location History.json'
    DATA_SOURCE_IMPORTANT_LOCATIONS = 'data/important_locations.csv'

    VIEW_DEFINITION = 'views/location.sql'

    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__()

    def extract(self):
        self.logger.info('Extract')
        assert(Path(LocationPipeline.DATA_SOURCE_LOCATION).is_file())
        assert(Path(LocationPipeline.DATA_SOURCE_IMPORTANT_LOCATIONS).is_file())

    def transform(self):
        self.logger.info('Transforming Location History')
        lh_parser = LocationHistoryParser(LocationPipeline.DATA_SOURCE_LOCATION)
        self.location = lh_parser.parse()
        self.important_locations = pd.read_csv(LocationPipeline.DATA_SOURCE_IMPORTANT_LOCATIONS)

    def load(self):
        self.logger.info('Loading Location History')
        engine = create_engine(self.secrets['mysql']['connector'])
        with engine.connect() as conn, conn.begin():
            engine.execute('DROP TABLE IF EXISTS location;')
            self.location.to_sql(name='location', con=engine, if_exists = 'replace', index=False)
            self.important_locations.to_sql(name='important_locations', con=engine, if_exists = 'replace', index=False)
            SQLPipeline.executeSQL(engine, self.VIEW_DEFINITION)
