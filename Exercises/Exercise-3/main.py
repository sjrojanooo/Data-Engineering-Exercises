from xmlrpc.client import gzip_decode
import boto3;
from botocore.exceptions import ClientError;
from dotenv import dotenv_values, load_dotenv;
import gzip;
import io; 
import os; 
import sys; 

# setting my config variable to hidden variables in .env file
config = {**dotenv_values('.env')}; 

# bucket name for s3 bucket;
BUCKET_NAME="commoncrawl";
# prefix key that we want from inside the bucket; 
PREFIX_KEY="crawl-data/CC-MAIN-2022-05/wet.paths.gz"; 

# setting up the boto3 client with my aws secret keys; 
# region name is important if you are quering s3 buckets in different regions; 
# you would want to set up the to the region that corresponds to that buckets region; 
s3 = boto3.client('s3',
        aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'],
        region_name = 'us-west-2'
);


def getBucketObject(bucketName, prefixKey):
    result = s3.get_object(Bucket=bucketName, Key=prefixKey);

    return result;


# method that will return the first item key from the results returned 
def getTargetS3Keys(bucketName, prefixKey):

    # getting the list of objects from the method 
    # parameters are the bucket name and prefix key from above; 
    result = getBucketObject(bucketName,prefixKey);

    lines = [];

    with gzip.open(result['Body'], 'rt') as gf:

        for line in gf:
            lines.append(line);

    fileName = lines[0].strip().split('/')[2:6];
    fileName = '/'.join(fileName);

    targetFile = getBucketObject(bucketName,lines[0].strip());

    file_content = None; 

    with gzip.open(targetFile['Body'], mode="rt") as f:
        file_content = f.read()


    return file_content

def main():

    try:

        file = getTargetS3Keys(BUCKET_NAME,PREFIX_KEY);

        print(file)

    except ClientError as e:

        if e.response['Error']['Code'] == "404":
            print("The object does not exist")

        else: 
            raise

    pass

if __name__ == "__main__":
    main();