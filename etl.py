import sys
import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from pipelines.AcademicsPipeline import AcademicsPipeline
from pipelines.LocationPipeline import LocationPipeline
from pipelines.FinancePipeline import FinancePipeline
from pipelines.MediaPipeline import MediaPipeline

def main():
    logging.basicConfig(format='[%(module)s.%(funcName)s] %(message)s',
                    datefmt='%d-%m-%Y:%H:%M:%S',
                    level=logging.INFO)

    PIPELINES = {
        'a': AcademicsPipeline,
        'l': LocationPipeline,
        'f': FinancePipeline,
        'm': MediaPipeline
    }

    if len(sys.argv) > 1:
        args = list(map(lambda x: x.replace('-', ''), sys.argv[1:]))
        for p in args:
            if p not in PIPELINES:
                logging.error('Invalid argument')
            else:
                pipeline = PIPELINES[p]()
                pipeline.extract()
                pipeline.transform()
                pipeline.load()
    else:
        # Execute everything
        for p in PIPELINES.values():
            pipeline = p()
            pipeline.extract()
            pipeline.transform()
            pipeline.load()

if __name__ == '__main__':
    main()
