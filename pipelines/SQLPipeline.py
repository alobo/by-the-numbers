import warnings
from pipelines.Pipeline import Pipeline

class SQLPipeline(Pipeline):
    """Workflow which loads data into a SQL DB

    Contains convenience methods which make working with SQL data easy.
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def _processSQL(sql):
        """Preprocess raw SQL to ensure the SQL driver can execute it.

        This is quite a hack, but I don't want to convert my SQL to sqlalchmey statements
        """

        # string processing to remove comments and control characters
        sql = filter(lambda x: '--' not in x, sql)
        sql = ''.join(sql).replace('\n', ' ').replace('\t', ' ')
        sql = sql.replace('%', '%%') # escape % so the python sql driver doesn't get confused
        sql = sql.split(';')
        sql = map(lambda x: x.strip(), sql)
        sql = filter(len, sql)
        sql = map(lambda x: x + ';', sql)
        return list(sql)

    @staticmethod
    def executeSQL(engine, sqlfile):
        """Class methods are similar to regular functions.

        Note:
            Do not include the `self` parameter in the ``Args`` section.

        Args:
            param1: The first parameter.
            param2: The second parameter.

        Returns:
            True if successful, False otherwise.
        """

        with open(sqlfile) as f:
            sql = f.readlines()

        sql = SQLPipeline._processSQL(sql)

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            for statement in sql:
                engine.execute(statement)
