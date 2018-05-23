import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

class WatcardParser:
    """Parse Watcard Accont Activity from saved webpage"""

    def __init__(self, filename):
        with open(filename) as f:
            self.soup = BeautifulSoup(f.read(), 'html.parser')
            assert('Transactions' in self.soup.title)

    def parse(self):
        table = self.soup.findAll('table', class_='table table-striped ow-table-responsive')
        df = pd.read_html(str(table[0]))[0]

        # Clean the data
        df.columns = ['date', 'amount', 'balance', 'units', 'memo', 'description']
        df['date'] = pd.to_datetime(df['date'])
        df['amount'] = pd.to_numeric(df['amount'].str.replace('$', ''))
        df['account'] = 'Watcard'
        df['category'] = np.nan
        df = df[['date', 'description', 'account', 'memo', 'amount', 'category']]

        # Identify tim hortons purchases
        tims = df.description.str.contains('TH-') | df.description.str.contains('THX-')| df.description.str.contains('TIM')
        df.loc[tims, 'description'] = 'UW TIM HORTONS'
        df.loc[tims, 'category'] = 'Personal:Dining'

        # Classify remaining transactions
        df.loc[df.description.str.contains('UPRINT'), 'category'] = 'Education'
        df.loc[df.category.isnull() & ~df.memo.str.contains('PREPAYMENT'), 'category'] = 'Personal:Dining'

        return df
