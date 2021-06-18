import sqlite3
from sqlite3 import Error
from pathlib import Path
import pandas as pd
import logging


class dbHandler():

    def __init__(self):
        self.conn = None
        self.cursor = None

    def createConnection(self, db_file: str) -> sqlite3.Connection:
        Path(db_file).touch()
        try:
            self.conn = sqlite3.connect(db_file)
            self.cursor = self.conn.cursor()
            logging.info('Connection to {} successfully created'.format(db_file))
            return self.conn
        except Error as e:
            print(e)

    def createCursor(self):
        try:
            return self.conn.cursor()
        except Error as e:
            print(e, "Connection not established")

    def createTableFromCsv(self, tableName: str, schema: str, csvPath: str):
        try:
            self.conn.execute("DROP TABLE IF EXISTS `{}`".format(tableName))
            self.conn.execute("CREATE TABLE {} {}".format(tableName, schema))
            pd.read_csv(csvPath).to_sql(tableName, self.conn, if_exists='replace', index=False)
            logging.info('Table {} filled from {}'.format(tableName, csvPath))
        except Error as e:
            print(e)
            raise RuntimeError
            # if self.conn:
            #     self.conn.close()

    def renameColumn(self, tableName: str, currentName: str, newName: str):
        query = '''
        ALTER TABLE {}
        RENAME COLUMN {} TO {}
        '''.format(tableName, currentName, newName)
        try:
            self.conn.execute(query)
        except Error as e:
            print(e)
            raise RuntimeError
            # if self.conn:
            #     self.conn.close()

    def fix_table(self, tableName: str, currentName: str, newName: str):
        query = '''
        ALTER TABLE {}
        RENAME COLUMN {} TO {}
        '''.format(tableName, currentName, newName)
        self.conn.execute(query)

    def convertTableIntoPandasDataFrame(self, tableName: str) -> pd.DataFrame:
        try:
            return pd.read_sql('select * from {}'.format(tableName), self.conn)
        except Error as e:
            print(e)
            raise RuntimeError
            # if self.conn:
            #     self.conn.close()

    def executeQueryWithResult(self, query: str):
        try:
            return pd.read_sql(query, self.conn)
        except Error as e:
            print(e)
            raise RuntimeError
            # if self.conn:
            #     self.conn.close()

    def executeQueryWithoutResult(self, query: str):
        try:
            self.conn.cursor().execute(query)
        except Error as e:
            print(e)
            raise RuntimeError
            # if self.conn:
            #     self.conn.close()

    def castColumns(self, columnName: str):
        self.cursor.execute('select cast( {} as real );'.format(columnName))

    def cleanUp(self):
        if self.conn:
            self.conn.close()
