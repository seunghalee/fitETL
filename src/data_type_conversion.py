import ast
from config import *


def get_redshift_data_type(val, current_type):
    """
    Takes in value and its data type,
    evaluates it and then returns appropriate Redshift data type
    """

    try:
        # safely evaluates value and strings any errors
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'
    if type(t) in [int, float]:
        if type(t) is int and current_type not in ['float64', 'varchar']:
            # use smallest possible int type
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


def create_table_in_redshift(reader, table_name):
    # returns SQL statement to create table in Redshift for given table
    headers = []
    type_list = []
    longest = []

    for row in reader:
        if len(headers) == 0:
            headers = row
            for col in headers:
                type_list.append('')
                longest.append(1)
        else:
            for i in range(len(row)):
                # NA is the csv null value
                if type_list[i] == 'varchar' or row[i] == 'NA':
                    pass
                else:
                    var_type = get_redshift_data_type(row[i], type_list[i])
                    type_list[i] = var_type

                if len(row[i]) > longest[i]:
                    # redshift column width limit is 65535 bytes
                    if len(row[i]) > 65535:
                        longest[i] = 65535
                    else:
                        longest[i] = len(row[i]) + 50

    statement = 'create table ' + table_name + ' ('
    for i in range(len(headers)):
        if type_list[i] == 'varchar':
            statement = (statement + '\n \"{column_name}\" varchar({size}),')\
                .format(column_name=str(headers[i].lower()), size=str(longest[i]))
        else:
            statement = (statement + '\n' + '\"{column_name}\" {data_type}' + ',')\
                .format(column_name=str(headers[i].lower()), data_type=type_list[i])
    statement = statement[:-1] + ');'

    return statement


def copy_table(table_name, file_name):
    # copy table contents from file
    sql = """copy {table_name} from 's3://{bucket_name}/{file_name}'
            access_key_id '{access_key_id}'
            secret_access_key '{secret_access_key}'
            region '{region}'
            ignoreheader 1
            null as 'NA'
            blanksasnull
            emptyasnull
            CSV
            ACCEPTINVCHARS
            TRUNCATECOLUMNS
            delimiter ',';""".format(table_name=table_name,
                                     bucket_name=bucketname,
                                     file_name=file_name,
                                     access_key_id=aws_access_key_id,
                                     secret_access_key=aws_secret_access_key,
                                     region=aws_region)
    return sql
