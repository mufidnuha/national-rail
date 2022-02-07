import boto3
import re
import os

AWS_KEY_ID = 'AKIAIPNSD2N5FJC5CMSQ'
AWS_SECRET = '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'
AWS_REGION = 'eu-west-1'

def ingest(date):
    bucket_name = 'darwin.xmltimetable'
    file_dir = 'PPTimetable/'
    src_dir = os.getcwd()
    dest_dir = src_dir+'/mnt/data_lake/landing/'+file_dir+date+'/'
    s3 = boto3.client('s3',
                        region_name=AWS_REGION,
                        aws_access_key_id=AWS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET)

    
    response = s3.list_objects(Bucket= bucket_name, 
                                Prefix= file_dir + date)
                            
    for obj in response['Contents']:
        splits = re.split("/", obj['Key'])
        s3.download_file(bucket_name, obj['Key'], dest_dir+splits[1])


