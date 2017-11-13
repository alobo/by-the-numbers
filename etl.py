import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from pipelines.AcademicsPipeline import AcademicsPipeline

def main():
    logging.basicConfig(format='[%(module)s.%(funcName)s] %(message)s',
                    datefmt='%d-%m-%Y:%H:%M:%S',
                    level=logging.DEBUG)

    a = AcademicsPipeline()
    a.extract()
    a.transform()
    a.load()

if __name__ == '__main__':
    main()