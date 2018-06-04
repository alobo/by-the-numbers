import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

from pipelines.SQLPipeline import SQLPipeline
from parsers.google.CalendarParser import CalendarParser
from parsers.uwaterloo.EmailParser import EmailParser
from parsers.uwaterloo.TranscriptParser import TranscriptParser

class AcademicsPipeline(SQLPipeline):

    DATA_SOURCE_SCHEDULE = 'data/google/Calendar/school-schedule.ics'
    DATA_SOURCE_TRANSCRIPT = 'data/uwaterloo/transcript.csv'
    DATA_SOURCE_IMPORTANT_DATES = 'data/uwaterloo/important_dates.csv'
    DATA_SOURCE_EMAIL = ['data/uwaterloo/mail/Inbox.mbox', 'data/uwaterloo/mail/Archive.mbox', 'data/uwaterloo/mail/Gmail-Fwd.mbox']

    VIEW_DEFINITION = 'sql/academics.sql'

    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__()

    def extract(self):
        self.logger.info('Extract')
        assert(Path(AcademicsPipeline.DATA_SOURCE_SCHEDULE).is_file())
        assert(Path(AcademicsPipeline.DATA_SOURCE_TRANSCRIPT).is_file())
        assert(Path(AcademicsPipeline.DATA_SOURCE_IMPORTANT_DATES).is_file())
        for email_archive in AcademicsPipeline.DATA_SOURCE_EMAIL:
            assert(Path(email_archive).is_file())

    def transform(self):
        self.logger.info('Processing Schedule')
        calendar_parser = CalendarParser(AcademicsPipeline.DATA_SOURCE_SCHEDULE)
        self.schedule = pd.DataFrame(calendar_parser.parse())
        self.schedule = self.schedule.sort_values(by='start').reset_index(drop=True)

        self.logger.info('Processing Important Dates')
        self.important_dates = pd.read_csv(AcademicsPipeline.DATA_SOURCE_IMPORTANT_DATES)
        # Ensure we include the last date
        self.important_dates['start'] += ' 00:00:00'
        self.important_dates['end'] += ' 23:59:59'
        self.important_dates['start'] = pd.to_datetime(self.important_dates['start'])
        self.important_dates['end'] = pd.to_datetime(self.important_dates['end'])

        self.logger.info('Processing Transcript')
        transcript_parser = TranscriptParser(AcademicsPipeline.DATA_SOURCE_TRANSCRIPT)
        self.transcript = pd.DataFrame(transcript_parser.parse())

        self.logger.info('Processing Email')
        self.email = pd.DataFrame()
        for email_archive in AcademicsPipeline.DATA_SOURCE_EMAIL:
            email_parser = EmailParser(email_archive)
            self.email = self.email.append(email_parser.parse())

        self.email = self.email.drop_duplicates(subset=['date', 'subject'], keep='first')
        self.email = self.email.sort_values(by='date').reset_index(drop=True)

    def load(self):
        engine = create_engine(self.secrets['mysql']['connector'])

        with engine.connect() as conn, conn.begin():
            self.logger.info('Loading Schedule')
            # Hack to strip TZ info before SQL insert
            self.schedule.start = (self.schedule.start).apply(lambda d: pd.to_datetime(str(d)))
            self.schedule.end = (self.schedule.end).apply(lambda d: pd.to_datetime(str(d)))
            self.schedule.to_sql(name='schedule', con=engine, if_exists = 'replace', index=False)

            self.logger.info('Loading Important Dates')
            self.important_dates.to_sql(name='important_dates', con=engine, if_exists = 'replace', index=False)

            self.logger.info('Loading Transcript')
            self.transcript.to_sql(name='transcript', con=engine, if_exists = 'replace', index=False)

            self.logger.info('Loading Email')
            self.email.to_sql(name='email', con=engine, if_exists = 'replace', index=False)

            SQLPipeline.executeSQL(engine, self.VIEW_DEFINITION)
