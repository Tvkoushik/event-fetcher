import json
import os
import zipfile
import boto3

s3 = boto3.client('s3')
aws_lambda = boto3.client('lambda')

s3_bucket_name = os.environ.get('S3_BUCKET')
library = os.environ.get('LIBRARY')

def lambda_handler(event, context):
    os.mkdir("/tmp/python")
    
    os.system('pip install -t /tmp/python ' + library)
    os.system('rm -rf /tmp/python/__pycache__')

    zip_path = '/tmp/' + library + '_layer.zip'
    
    zipf = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED)

    for root, dirs, files in os.walk('/tmp/python/'):
        for file in files:
            fpath = os.path.join(root, file)
            zipf.write(fpath, arcname=fpath.replace('/tmp',''))
 
    zipf.close()
    
    s3.upload_file(
        Filename=zip_path,
        Bucket=s3_bucket_name,
        Key='layers/' + library + '_layer.zip'
    )
    
    # response = aws_lambda.publish_layer_version(
    #     CompatibleRuntimes=[
    #         'python3.10'
    #     ],
    #     Content={
    #         'S3Bucket': s3_bucket_name,
    #         'S3Key': 'layers/' + library + '_layer.zip'
    #     },
    #     LayerName=library + '_layer',
    #     Description='Created From Lambda'
    # )