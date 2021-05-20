import pandas as pd
import boto3
import io
from io import StringIO

# Function to load all files with the same name pattern

def read_multiple_csv(bucket, prefix, encoding):
    """
    This function reads several .csv files with the same name pattern from the S3 bucket
    and extracts the month of the report from the filename. 
    """
    resource = boto3.resource('s3')
    bucket = resource.Bucket(bucket)
    objects = bucket.objects.filter(Prefix=prefix)
    df = pd.concat((pd.read_csv(io.BytesIO(obj.get()['Body'].read()), encoding = encoding).\
                    #column with the month of the report
                    assign(MONTH = obj.key[len(obj.key)-12:-8] + '/' + obj.key[len(obj.key)-8:-6]) for obj in objects),\
                   ignore_index=True)
    return df


# Function to put pandas dataframe to csv file on S3 bucket

def df_to_csv(df, bucket, filename):
    """
    This function puts a pandas dataframe into .csv file on the S3 bucket. 
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())
