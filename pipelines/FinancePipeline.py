import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from pipelines.SQLPipeline import SQLPipeline
from parsers.finance.FinanceParser import FinanceParser
from parsers.finance.WatcardParser import WatcardParser

class FinancePipeline(SQLPipeline):

    DATA_SOURCE_FINANCE = 'data/finance/bank.tsv'
    DATA_SOURCE_WATCARD = 'data/uwaterloo/watcard/watcard.htm'

    VIEW_DEFINITION = 'sql/finance.sql'

    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__()

    def extract(self):
        self.logger.info('Extract')
        assert(Path(FinancePipeline.DATA_SOURCE_FINANCE).is_file())
        assert(Path(FinancePipeline.DATA_SOURCE_WATCARD).is_file())

    def transform(self):
        self.logger.info('Transforming Finances')
        fp_parser = FinanceParser(FinancePipeline.DATA_SOURCE_FINANCE)
        self.finances = fp_parser.parse()

        wp_parser = WatcardParser(FinancePipeline.DATA_SOURCE_WATCARD)
        self.finances = self.finances.append(wp_parser.parse())

        self.finances = self.finances.sort_values(by='date')

    def load(self):
        self.logger.info('Loading Finances')
        engine = create_engine(self.secrets['mysql']['connector'])
        with engine.connect() as conn, conn.begin():
            self.finances.to_sql(name='finances', con=engine, if_exists = 'replace', index=False)
            # create views
            SQLPipeline.executeSQL(engine, self.VIEW_DEFINITION)