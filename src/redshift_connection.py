import psycopg2
from config import *


class RedshiftConnection:

    def __init__(self):
        self.redshift_host = redshift_host
        self.redshift_db = redshift_db
        self.redshift_port = redshift_port
        self.redshift_user = redshift_user
        self.redshift_pw = redshift_pw

    def connect_to_redshift(self):
        # Redshift connection object
        self.conn = psycopg2.connect(host=self.redshift_host,
                                     dbname=self.redshift_db,
                                     port=self.redshift_port,
                                     user=self.redshift_user,
                                     password=self.redshift_pw)
        return self.conn

    def create_cursor(self):
        # create cursor that can execute queries on Redshift database
        self.connect_to_redshift()
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

    def delete_existing_tables(self):
        tables = self.all_tables()
        for table in tables:
            self.cursor.execute("drop table \"" + str(table) + "\"")
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()
