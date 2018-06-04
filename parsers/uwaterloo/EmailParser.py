import mailbox
import pandas as pd
from email.header import decode_header
from email.utils import parsedate_tz

class EmailParser:
    """Parse an mbox archive into a Pandas DataFrame

    Thanks to: https://www.grant-trebbin.com/2015/10/process-mbox-file-with-python.html
    """

    def __init__(self, filename):
        self.messages = mailbox.mbox(filename)

    def parse(self):
        parsed = []
        for message in self.messages:
            subject, encoding = decode_header(message['subject'])[0]
            if encoding: subject = subject.decode('ascii', 'ignore')

            froms, encoding = decode_header(message['from'])[0]
            if encoding: froms = froms.decode('ascii', 'ignore')

            parsed.append([message['Date'], froms, subject])

        df = pd.DataFrame(parsed)
        df.columns = ['date', 'from', 'subject']
        df['date'] = pd.to_datetime(df['date'])

        return df
