import boto3


def connect_to_s3():
    # instantiate s3 client
    return boto3.client('s3')


def bucket_names():
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        print(bucket.name)
