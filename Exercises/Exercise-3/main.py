from dotenv import dotenv_values, load_dotenv
import boto3
from botocore.exceptions import ClientError
import gzip

# setting my config variable to hidden variables in .env file
config = {**dotenv_values('.env')}; 

# bucket name for s3 bucket;
BUCKET_NAME="commoncrawl";
# prefix key that we want from inside the bucket; 
KEY="crawl-data/CC-MAIN-2022-05/wet.paths.gz"; 

# setting up the boto3 client with my aws secret keys; 
# region name is important if you are quering s3 buckets in different regions; 
# you would want to set up the to the region that corresponds to that buckets region; 
s3 = boto3.client('s3',
        aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'],
        region_name = 'us-west-2'
);


# function that will return the first item key from the results returned 
def getTargetS3Keys(bucketName, prefixKey):
    
    """
        Get list of keys and return the first element 
        in the array 
    """

    keys = []; 

    result = s3.list_objects_v2(Bucket=bucketName, Prefix=prefixKey);

    for obj in result['Contents']:

        keys.append(obj['Key'])

        return keys[0]


def main():

    try:

        file = getTargetS3Keys(BUCKET_NAME,KEY)

        print(file)


    except ClientError as e:

        if e.response['Error']['Code'] == "404":
            print("The object does not exist")

        else: 
            raise


    pass

if __name__ == "__main__":
    main();