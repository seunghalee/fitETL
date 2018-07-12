import psycopg2
from config import *


class PostgresConnection:

    def __init__(self):
        self.rds_host = rds_host
        self.rds_db = rds_db
        self.rds_port = rds_port
        self.rds_user = rds_user
        self.rds_pw = rds_pw

    def connect_to_postgres(self):
        # Postgres connection object
        self.conn = psycopg2.connect(host=self.rds_host,
                                     dbname=self.rds_db,
                                     port=self.rds_port,
                                     user=self.rds_user,
                                     password=self.rds_pw)
        return self.conn

    def create_cursor(self):
        # create cursor that can execute queries on Postgres database
        self.connect_to_postgres()
        self.cursor = self.conn.cursor()
        return self.cursor

    def run_query(self, sql_command):
        self.create_cursor()
        self.cursor.execute(sql_command)

    def run_query_commit(self, sql_command):
        self.run_query(sql_command)
        self.conn.commit()

    def all_tables(self):
        self.run_query("""SELECT table_name
                          FROM information_schema.tables
                          WHERE table_schema = 'public'""")
        output = self.cursor.fetchall()
        tables = []
        for table in output:
            tables.append(str(table)[2:-3])
        return tables

    def num_rows(self, table):
        self.run_query("SELECT COUNT(*) FROM \"" + str(table) + "\"")
        n = self.cursor.fetchone()[0]
        return n

    def close(self):
        self.cursor.close()
        self.conn.close()
