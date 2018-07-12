import csv
import sys
from datetime import datetime

from pytz import timezone
import pytz

from config import *
from postgres_connection import PostgresConnection
from s3_connection import *
from redshift_connection import RedshiftConnection
from data_type_conversion import *


def current_time():
    # get current time in PST
    date = datetime.now(tz=pytz.utc)
    date = date.astimezone(timezone('US/Pacific'))
    timestr = date.strftime("_%Y-%m-%d_%H:%M:%S")
    return timestr


def prevent_csv_overflow():
    maxInt = sys.maxsize
    decrement = True

    while decrement:
        # decrease the maxInt value by factor 10 when OverflowError occurs.
        decrement = False
        try:
            csv.field_size_limit(maxInt)
        except OverflowError:
            maxInt = int(maxInt/10)
            decrement = True


if __name__ == "__main__":

    pg_conn = PostgresConnection()
    pg_cur = pg_conn.create_cursor()
    tables = pg_conn.all_tables()
    print("LIST OF TABLES:")
    print(tables)
    print("\ntotal number of tables: " + str(len(tables)))

    s3 = connect_to_s3()

    # upload Postgres tables to Amazon S3 bucket
    files = []
    for table in tables:
        print(table)
        # only transfer non-empty tables
        if pg_conn.num_rows(table) != 0:
            query_all_data = "SELECT * from \"" + str(table) + "\""

            filename = str(table) + current_time() + '.csv'
            filename = filename.lower()
            files.append(filename)
            local_path = '/home/ubuntu/temp/' + filename

            sql_export_table = "COPY ({}) TO STDOUT WITH CSV HEADER".format(query_all_data)
            with open(local_path, 'w') as f:
                pg_cur.copy_expert(sql_export_table, f)

            s3.upload_file(local_path, bucketname, filename)

    pg_conn.close()
    print("\nFiles uploaded to s3 bucket\n")

    redshift_conn = RedshiftConnection()
    redshift_cur = redshift_conn.connect_to_redshift()
    redshift_conn.delete_existing_tables()

    prevent_csv_overflow()

    # load tables into Redshift from S3
    for file_name in files:
        try:
            f = open('/home/ubuntu/temp/' + file_name, 'r')
            reader = csv.reader(f)
            table_name = file_name[:-24]
            create_table_statement = create_table_in_redshift(reader, table_name)
            copy_table_statement = copy_table(table_name, file_name)
            f.close()
            redshift_conn.run_query_commit(create_table_statement)
            redshift_conn.run_query_commit(copy_table_statement)
            print("done migrating " + table_name)

        except Exception as e:
            print("ERROR: " + file_name)
            print(e)
            redshift_conn.run_query("rollback;")

    redshift_conn.close()
