import boto3; # importing AWS SDK to enable the S3 service; 
from botocore.exceptions import ClientError; # importing the boto3 service exception ClientError to catch general exceptions;
from dotenv import dotenv_values; # enabling advanced configuration management by loading .env variables in a dict format; 
import gzip; # gzip provides a simple interface to compress and decompress .gz files; 


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


# request to fetch the object by bucket name and key with boto clend 
def getBucketObject(bucketName, prefixKey):
    result = s3.get_object(Bucket=bucketName, Key=prefixKey);

    return result;

# initial request to common crawl archive
# method will return the first 
def captureSegmentTarget(result):
    segment = []; 

    with gzip.open(result['Body'], 'rt') as gf:
        for segments in gf:
            segment.append(segments)

    return segment[0].strip(); 

# Printing content from segment file target. 
def printContent(targetFile):
    file_content = None;

    with gzip.open(targetFile['Body'], mode='rt') as f:
        file_content = f.read();

    return file_content; 


def getTargetS3Keys(bucketName, prefixKey):

    # getting the list of objects from the method 
    # parameters are the bucket name and prefix key from above; 
    return printContent(targetFile = getBucketObject(bucketName,captureSegmentTarget(result = getBucketObject(bucketName,prefixKey))))

# main method that take in the parameters; 
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