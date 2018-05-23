import logging
from io import StringIO
import pandas as pd
import numpy as np

class FinanceParser:
    """Parse exported data from accounting software"""
    HEADER = '#Account	Account ID	Account Type	Currency	Start Balance	'

    logger = logging.getLogger(__name__)

    def __init__(self, filename):
        with open(filename) as f:
            self.lines = f.read()
        self.account_names = set()

    def parse(self):

        # Isolate my primary accounts
        accounts = self.lines.split(FinanceParser.HEADER)
        accounts = map(lambda x: x.split('\n'), accounts)
        accounts = filter(lambda x: 'Checking' in x[1] or 'Visa' in x[1], accounts)
        accounts = list(accounts)
        self.logger.debug('Found {} primary accounts:'.format(len(accounts)))

        # Parse accounts into dataframes
        account_dfs = []
        for account in accounts:
            # Strip account header and extract name
            account_name = account[1].split('\t')[0]
            self.account_names.add(account_name)
            data = StringIO('\n'.join(account[2:]))

            # Initial data cleaning
            account_dfs.append(pd.read_csv(data, sep='\t'))
            account_dfs[-1].columns = ['date', 'tax_date', 'entered_date', 'check_num', 'description', 'status', 'account', 'memo', 'amount', 'unnamed']
            account_dfs[-1] = account_dfs[-1].drop(['tax_date', 'status', 'check_num', 'unnamed'], axis=1)
            self.logger.debug(' {} (len: {})'.format(account_name, len(account_dfs[-1])))


        df_combined = pd.DataFrame()
        for df in account_dfs:
            df = df.replace('-', np.nan)

            # Assign each transaction an ID, only increment ID on valid date
            df['tid'] = df.date.notnull().astype(int).cumsum()

            # Groupby transaction ID and reduce to a single row
            df = df.groupby('tid').apply(self.reduce_transaction)
            df = df[pd.notnull(df['date'])]

            df_combined = df_combined.append(df)

        self.logger.debug('Total Merged Transactions: {}'.format(len(df_combined)))

        # Final data cleaning
        df_combined['date'] = pd.to_datetime(df_combined['date'])
        df_combined = df_combined.drop(['entered_date', 'tid'], axis=1)
        df_combined = df_combined.sort_values(by='date')
        df_combined = df_combined.reset_index(drop=True)
        df_combined

        return df_combined

    def reduce_transaction(self, df):
        """Reduce transaction to one row"""
        # Seperate transaction category from account name
        category = list(set(df.account) - self.account_names)
        if not len(category): return

        df['category'] = category[0]
        return df.iloc[0]
