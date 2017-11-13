import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

from pipelines.Pipeline import Pipeline
from parsers.google.CalendarParser import CalendarParser
from parsers.uwaterloo.TranscriptParser import TranscriptParser

class AcademicsPipeline(Pipeline):

    DATA_SOURCE_SCHEDULE = 'data/google/Calendar/school-schedule.ics'
    DATA_SOURCE_TRANSCRIPT = 'data/uwaterloo/transcript.txt'
    DATA_SOURCE_IMPORTANT_DATES = 'data/uwaterloo/important_dates.csv'

    logger = logging.getLogger(__name__)

    def __init__(self):
        Pipeline.__init__(self)

    def extract(self):
        self.logger.info('Extract')
        assert(Path(AcademicsPipeline.DATA_SOURCE_SCHEDULE).is_file())
        assert(Path(AcademicsPipeline.DATA_SOURCE_TRANSCRIPT).is_file())
        assert(Path(AcademicsPipeline.DATA_SOURCE_IMPORTANT_DATES).is_file())

    def transform(self):
        self.logger.info('Processing Schedule')
        calendar_parser = CalendarParser(AcademicsPipeline.DATA_SOURCE_SCHEDULE)
        self.schedule = pd.DataFrame(calendar_parser.parse())
        self.schedule = self.schedule.sort_values(by='start').reset_index(drop=True)

        self.logger.info('Processing Important Dates')
        self.important_dates = pd.read_csv(AcademicsPipeline.DATA_SOURCE_IMPORTANT_DATES)

        self.logger.info('Processing Transcript')
        transcript_parser = TranscriptParser(AcademicsPipeline.DATA_SOURCE_TRANSCRIPT)
        self.transcript = pd.DataFrame(transcript_parser.parse())

    def load(self):
        engine = create_engine('mysql://root@localhost/test')

        with engine.connect() as conn, conn.begin():
            self.logger.info('Loading Schedule')
            self.schedule.to_sql(name='schedule', con=engine, if_exists = 'replace', index=False)

            self.logger.info('Loading Important Dates')
            self.important_dates.to_sql(name='important_dates', con=engine, if_exists = 'replace', index=False)

            self.logger.info('Loading Transcript')
            self.transcript.to_sql(name='transcript', con=engine, if_exists = 'replace', index=False)
