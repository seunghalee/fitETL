import pandas as pd
from collections import defaultdict
from postgres_connection import PostgresConnection
from redshift_connection import RedshiftConnection
from utils import *


def not_match(list1, list2):
    # Return index of list item that doesn't match
    d = defaultdict(list)
    for index, item in enumerate(list1):
        d[item].append(index)

    return [d[item] for item in list1 if item not in list2]


if __name__ == "__main__":

    # identify empty Postgres tables and check rows for non-empty tables
    pg = PostgresConnection()
    pg_tables = pg.all_tables()
    pg_tables = sorted(pg_tables, key=str.lower)
    empty_tables = []
    pg_rows = []
    migrated_tables = []
    for table in pg_tables:
        num_rows = pg.num_rows(table)
        if num_rows == 0:
            empty_tables.append(table)
        else:
            migrated_tables.append(table)
            pg_rows.append(num_rows)
    pg.close()

    # check rows in Redshift
    rs = RedshiftConnection()
    rs_tables = rs.all_tables()
    rs_tables = sorted(rs_tables, key=str.lower)
    rs_rows = []
    for table in rs_tables:
        rs_rows.append(rs.num_rows(table))
    rs.close()

    # output table names and number of rows in Postgres and Redshift
    df = pd.DataFrame(data={'Table_Name': migrated_tables,
                            "Postgres": pg_rows, 'Redshift': rs_rows})
    columns_titles = ['Table_Name', 'Postgres', 'Redshift']
    df = df.reindex(columns=columns_titles)
    log = setup_logger(file_name=__file__, name=__name__)
    log.info(df)

    # check if number of rows match for all tables in Postgres and Redshift
    if not not_match(pg_rows, rs_rows):
        log.info("All tables have been successfully transferred!")

    else:
        log.error("TABLES THAT DO NOT MATCH:")
        for table in not_match(pg_rows, rs_rows):
            log.error(migrated_tables[int(table[0])])

    log.info("{s}{tables}".format(s="EMPTY POSTGRES TABLES: ",
                                  tables='\n'.join(empty_tables)))
