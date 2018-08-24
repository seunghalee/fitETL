## config.py
Save AWS credentials, which have been set as environmental variables, as Python variables.

## data_type_conversion.py
Module that contains methods to convert Postgres columns into data types supported in Redshift.

## postgres_connection.py
Module that contains methods to connect to Postgres and execute SQL statements. Get names of all Postgres tables in the database using `all_tables()` and get number of total rows in a given table using `num_rows(table_name)`.

## postgres_to_redshift.py
This is the working migration script that exports tables from Postgres to Redshift.

## redshift_connection.py
Module that contains methods to connect to Redshift and execute SQL statements. Get name of all Redshift tables in the database using `all_tables()` and get number of total rows in a given table using `num_rows(table_name)`. `delete_existing_table(table_name)` drops a given table -- during the migration process, each table is deleted and then recreated.

## s3_connection.py
Module that contains methods to establish a connection to S3, get all bucket names, and  download files from a given bucket.

## utils.py
Contains project directory and temporary directory (used for storing CSV files before they are loaded into S3). Also contains methods to set up Python logger.

## validation.py
Script to validate migration process. This script compares the tables in Postgres and in Redshift by checking the number of rows in both databases and logs any tables that do not have matching rows. The script also logs any empty Postgres tables (empty tables are not migrated to Redshift).
