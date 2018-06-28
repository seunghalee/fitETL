import psycopg2
import boto3
import gzip
import pandas as pd
from sh import pg_dump
from sqlalchemy import create_engine
import os, csv, ast 

##################### Fill in variables ########################
# AWS credentials
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
region = ""

# PostgreSQL
rds_host = ""
rds_user = ""
rds_pw = ""
rds_port = 5432
rds_db = ""

# S3
bucketname = ""

#Redshift
redshift_host = ""
redshift_db = "" #  default is dev
redshift_user = ""
redshift_pw = ""
redshift_port = 5439
#########################################################

def dataType(val, current_type):
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'
    if type(t) in [int, float]:
        if type(t) is int and current_type not in ['float64', 'varchar']:
           # Use smallest possible int type
            if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                return 'smallint'
            elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                return 'int'
            else:
                return 'bigint'
        if type(t) is float and current_type not in ['varchar']:
           return 'decimal'
    else:
        return 'varchar'

# connect to Postgres database and get a connection object
conn = psycopg2.connect(host=rds_host,
                        dbname=rds_db,
                        port=rds_port,
                        user=rds_user,
                        password=rds_pw)

# create a psycopg2 cursor that can execute queries on Postgres database
cur = conn.cursor()

# instantiate s3 client -- CSV files will be saved in S3
s3 = boto3.client('s3')

# get all bucket names
#for bucket in s3.buckets.all():
#    print(bucket.name)

# get a list of all tables
cur.execute("""SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'""")
tables = []
for table in cur.fetchall():
    tables.append(str(table)[2:-3])

#print("LIST OF TABLES:")
#print(tables)
#print("")
#print("total number of tables: " + str(len(tables)))
#print("")

# load table into Pandas dataframe
files = []
for table in tables:
    # check if table is empty
    cur.execute("SELECT COUNT(*) from \"" + str(table) + "\"")
    num_rows = cur.fetchone()[0]

    # only transfer non-empty tables
    if num_rows != 0:
        query_sql = "SELECT * from \"" + str(table) + "\""
        df = pd.read_sql(query_sql, con=conn)

        # save the tables as csv files in temporary local folder
        filename = str(table) + '.csv'
        files.append(filename)
        local_path = '/home/ubuntu/temp/' + filename
        df.to_csv(local_path, index=False, encoding='utf-8')

        # upload csv files to s3 bucket
        s3.upload_file(local_path, bucketname, filename)

# close cursor and connection
cur.close()
conn.close()

print("")
print("Files uploaded to s3 bucket")

# connect to Redshift database and get a connection object
conn = psycopg2.connect(host=redshift_host,
                        dbname=redshift_db,
                        port=redshift_port,
                        user=redshift_user,
                        password=redshift_pw)

# create a psycopg2 cursor that can execute queries
cur = conn.cursor()

# SQLAlchemy (ORM) -- this isn't necessary; can just use psycopg2 
engine_str = "postgresql+psycopg2://%s:%s@%s:%d/%s" \
                % (redshift_user, redshift_pw, redshift_host, redshift_port, redshift_db)
engine = create_engine(engine_str)

# delete any existing tables in Redshift
cur.execute("""SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'public'""")
for table in cur.fetchall():
    cur.execute("drop table \"" + str(table)[2:-3]  + "\"")

conn.commit()

# generate SQL command to create tables saved as csv files
for item in files:
    try:
        f = open(item,'r')
        reader = csv.reader(f)

        longest, headers, type_list = [], [], []

        for row in reader:
            if len(headers) == 0:
                headers = row
                for col in row:
                    longest.append(1)
                    type_list.append('')
            else:
                for i in range(len(row)):
                # NA is the csv null value
                    if type_list[i] == 'varchar' or row[i] == 'NA':
                        pass
                    else:
                        var_type = dataType(row[i], type_list[i])
                        type_list[i] = var_type
                    if len(row[i]) > longest[i]:
                        longest[i] = len(row[i]) + 50
        f.close()

        statement = 'create table ' + item[:-4] + ' ('

        for i in range(len(headers)):
            if type_list[i] == 'varchar':
                statement = (statement + '\n\"{}\" varchar({}),').format(str(headers[i].lower()), str(longest[i]))
            else:
                statement = (statement + '\n' + '\"{}\" {}' + ',').format(str(headers[i].lower()), type_list[i])

        statement = statement[:-1] + ');'
        # print("")
        # print(statement)
        # print("")
        # print("creating table...")
        cur.execute(statement)
        conn.commit()
        # copy table
        # print("")
        # print("copying table " + str(item[:-4]))
        
        sql = """copy {table_name} from 's3://{bucket_name}/{file_name}'
                access_key_id '{access_key_id}'
                secret_access_key '{secret_access_key}'
                region '{region}'
                ignoreheader 1
                null as 'NA'
                ACCEPTINVCHARS
                TRUNCATECOLUMNS
                delimiter ',';""".format(access_key_id=AWS_ACCESS_KEY_ID, secret_access_key=AWS_SECRET_ACCESS_KEY, region=region,
                        table_name=item[:-4], bucket_name=bucketname, file_name=item)

        cur.execute(sql)
        conn.commit()
       # print("done copying " + str(item[:-4]))

    except Exception as e:
        print("ERROR")
        print(e)

# close the connection and cursor
cur.close()
conn.close()

