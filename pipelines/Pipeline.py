import json
import logging
from abc import ABCMeta, abstractmethod

class Pipeline(metaclass=ABCMeta):
    """Defines an abstract three-phase data workflow
    """

    def __init__(self):
        logger = logging.getLogger(__name__)
        logger.info('Loading Secrets')
        with open('data/secrets.json') as f:
            self.secrets = json.load(f)

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass
