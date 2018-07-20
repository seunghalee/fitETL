# fitETL: pushing updates from production to a data warehouse
As an Insight Data Engineering Fellow, I worked on a consulting project for a health science company. The project involved developing a batch processing pipeline for pushing updates from production to a data warehouse for future analysis.

## Motivation
The company is collecting large amounts of data from multiple sources. Currently, the data is stored in Postgres tables on Amazon RDS but to run complex analyses efficiently, the company wants to move its data to Amazon Redshift.

## Pipeline
![alt text](https://github.com/seunghalee/fitETL/blob/master/img/pipeline.png "ETL Pipeline")

1. The pipeline starts with over 50 tables in a PostgreSQL database, which contains live production data.
2. The PostgreSQL tables are exported as CSV files and loaded into Amazon S3. This helps create an audit trail of the files that are being migrated.
3. The tables are unloaded from S3 and recreated in Amazon Redshift. Unsupported PostgreSQL data types are converted to data types supported on Redshift (e.g. uuid → varchar).
