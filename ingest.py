import boto3
import re

AWS_KEY_ID = 'AKIAIPNSD2N5FJC5CMSQ'
AWS_SECRET = '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'
AWS_REGION = 'eu-west-1'

def ingest_xml(date):
    bucket_name = 'darwin.xmltimetable'
    folder_s3 = 'PPTimetable/'
    folder_local = '/Users/mufidnuha/Desktop/national_rail/PPTimetable/'
    s3 = boto3.client('s3',
                        region_name=AWS_REGION,
                        aws_access_key_id=AWS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET)

    
    response = s3.list_objects(Bucket= bucket_name, 
                                Prefix= folder_s3 + date)
                            
    for obj in response['Contents']:
        splits = re.split("/", obj['Key'])
        s3.download_file(bucket_name, obj['Key'], folder_local+date+'/'+splits[1])