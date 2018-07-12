import os

# AWS credentials
aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ["AWS_DEFAULT_REGION"]

# PostgreSQL
rds_host = os.environ["RDS_HOST"]
rds_user = os.environ["RDS_USER"]
rds_pw = os.environ["RDS_PASS"]
rds_port = os.environ["RDS_PORT"]
rds_db = os.environ["RDS_DB_NAME"]

# S3
bucketname = os.environ["S3_BUCKET_NAME"]

# Redshift
redshift_host = os.environ["REDSHIFT_HOST"]
redshift_db = os.environ["REDSHIFT_DB"]
redshift_user = os.environ["REDSHIFT_USER"]
redshift_pw = os.environ["REDSHIFT_PASS"]
redshift_port = os.environ["REDSHIFT_PORT"]
