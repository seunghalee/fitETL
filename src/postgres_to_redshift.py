import csv
import sys
from datetime import datetime

from pytz import timezone
import pytz

from config import *
from utils import *
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
    pg = PostgresConnection()
    pg_cur = pg.create_cursor()
    tables = pg.all_tables()
    log = setup_logger(file_name=__file__, name=__name__)
    log.info("{s} {l}".format(s="LIST OF TABLES: ", l=tables))
    log.info("Total number of tables: " + str(len(tables)))

    s3 = connect_to_s3()

    # upload Postgres tables to Amazon S3 bucket
    files = []
    for table in tables:
        # only transfer non-empty tables
        if pg.num_rows(table) != 0:
            query_all_data = "SELECT * from \"" + str(table) + "\""

            filename = str(table) + current_time() + '.csv'
            filename = filename.lower()
            files.append(filename)
            local_path = '/home/ubuntu/temp/' + filename

            sql_export_table = "COPY ({}) TO STDOUT WITH CSV HEADER".format(query_all_data)
            with open(local_path, 'w') as f:
                pg_cur.copy_expert(sql_export_table, f)

            s3.upload_file(local_path, bucketname, filename)

    pg.close()
    log.info("Files uploaded to s3 bucket")

    rs = RedshiftConnection()
    prevent_csv_overflow()
    # load tables from S3 into Redshift
    for file_name in files:
        try:
            f = open(PROJ_DIR + "temp/" + file_name, 'r')
            reader = csv.reader(f)
            table_name = file_name[:-24]
            rs.delete_existing_tables(table_name)
            create_table_statement = create_table_in_redshift(reader, table_name)
            copy_table_statement = copy_table(table_name, file_name)
            f.close()
            rs.run_query_commit(create_table_statement)
            rs.run_query_commit(copy_table_statement)
            log.info("Done migrating " + str(table_name))

        except Exception as e:
            log.error("{file}: {message}".format(file=file_name,
                                                 message=e))
            rs.run_query("rollback;")

    rs.close()
