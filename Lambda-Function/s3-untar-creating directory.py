from __future__ import print_function
import urllib
import json
import logging
import boto3
import re, datetime
import os
import gzip
import botocore
import tarfile
from tarfile import TarInfo
from botocore.client import Config
import io

#s3 = boto3.client('s3')
s3=boto3.client('s3', config=Config(signature_version='s3v4'))
s3_resource=boto3.resource('s3')

def lambda_handler(event, context):
    target_bucket ='dhi-adobe-analytics-dev-des'
    # setup constants
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    print (key)
   
    
    
    
    file_name_match =  re.search('diceprod', str(key))
    if  re.search('diceprod', str(key)):
        match = re.search('\d{4}-\d{2}-\d{2}', str(key))
        date1 = datetime.datetime.strptime(match.group(), '%Y-%m-%d').date()
        date2 = date1.strftime('%Y%m%d') 
        date2 ="date="+date2
        response = s3.put_object(Bucket=target_bucket, Key='events/' + date2 + '/web/')
        print (file_name_match)
        target_key = 'events/' + date2 + '/web/'
        copy_source = {'Bucket' :bucket, 'Key' :key}
        waiter = s3.get_waiter('object_exists')
        waiter.wait(Bucket=bucket, Key=key)
        s3.copy_object(Bucket=target_bucket, Key=target_key+key , CopySource=copy_source,)
    elif re.search('lookup', str(key)): 
        print("lookup file is here.....")
        print (key)
        new_bucket='dhi-adobe-analytics-dev-src' #new bucket name
        new_key=key[:-4]
        try:
            s3.download_file(bucket, key, '/tmp/file')
            if(tarfile.is_tarfile('/tmp/file')):
               tar = tarfile.open('/tmp/file', "r:gz")
               for TarInfo in tar:
                   tar.extract(TarInfo.name, path='/tmp/extract/')
                   s3.upload_file('/tmp/extract/'+TarInfo.name,new_bucket, TarInfo.name)
            tar.close()
        except Exception as e:
            print(e)
            raise e
           
