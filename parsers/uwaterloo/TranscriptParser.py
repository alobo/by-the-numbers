import pandas as pd

class TranscriptParser:

    def __init__(self, filename):
        self.transcript = pd.read_csv(filename)

    def parse(self):
        return self.transcript

