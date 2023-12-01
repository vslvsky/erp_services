import pandas as pd
import psycopg2
from typing import Tuple

from .env import *

class PostgreSQL():
    def __init__(self):
        self.__connection = psycopg2.connect(database=DATABASE,
                                             user=USER,
                                             password=PASS,
                                             port=PORT,
                                             host=HOST)
        self.__cursor = self.__connection.cursor()

    def __del__(self):
        self.__cursor.close()
        self.__connection.close()

    def set_connections(self) -> Tuple:
        self.C = (self.__connection, self.__cursor)
        return self.C


class Relational_DB:
    @staticmethod
    def get_dataframe(sql: str, connections: Tuple) -> pd.DataFrame:
        return pd.read_sql(sql, connections[0])

    @staticmethod
    def insert_dataframe(sql: str, connections: Tuple, dataframe: pd.DataFrame) -> None:
        connections[1].executemany(sql, dataframe.values)
        connections[0].commit()

    @staticmethod
    def sql_commands(sql: str, connections: Tuple) -> None:
        connections[1].execute(sql)
        connections[0].commit()

    @staticmethod
    def insert_row_dataframe(sql: str, connections: Tuple, dataframe: pd.DataFrame) -> None:
        columns = list(dataframe.columns)
        size = dataframe[columns[0]].size
        array = dataframe.to_numpy()

        for i in range(0, size):
            connections[1].execute(sql, array[i, :])
            connections[0].commit()