import boto3


BUCKET_NAME = 'sissa-historic-daily-forecast' 
PATH = 'ERA5/rain/2014.nc' 
s3 = boto3.resource('s3')

try:
    s3.Bucket(BUCKET_NAME).download_file(PATH, './rain_2014.nc')
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise
