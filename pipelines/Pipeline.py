import json
import logging
from abc import ABCMeta, abstractmethod

class Pipeline(metaclass=ABCMeta):

    VIEW_DEFINITION = None
    VIEW_DEFINITION_SQL = None

    def __init__(self):
        logger = logging.getLogger(__name__)
        logger.info('Loading Secrets')
        with open('data/secrets.json') as f:
            self.secrets = json.load(f)

        # If this pipeline creates views, preprocess the SQL
        # This is quite a hack, but I don't want to convert my SQL to sqlalchmey statements
        if self.VIEW_DEFINITION:
            with open(self.VIEW_DEFINITION) as f:
                lines = f.readlines()

            # string processing to remove comments and control characters
            lines = filter(lambda x: '--' not in x, lines)
            lines = ''.join(lines).replace('\n', ' ').replace('\t', ' ')
            lines = lines.replace('%', '%%') # escape % so the python sql driver doesn't get confused
            lines = lines.split(';')
            lines = map(lambda x: x.strip(), lines)
            lines = filter(len, lines)
            lines = map(lambda x: x + ';', lines)
            self.VIEW_DEFINITION_SQL = ''.join(lines)

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass
