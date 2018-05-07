#code written by gbutzi ask him about it unless the question is "why is your python so gross"

import boto3
import datetime
import json
import uuid
import os
import base64
from botocore.vendored import requests

# Documented for my own sanity in the future:
# When you encrypt() some data with KMS, it gives you a binary blob that's base64 encoded.
# If you want to then decrypt this blob later, you need to pass bytes to kms.decrypt().
# The way to get the bytes you need is to base64 decode the base64 encoded data; don't just call
# python's decode() on the data or KMS will give you an utterly inscrutable type error.

kms = boto3.client('kms')
encrypted_auth = os.environ['auth']
binary_auth = base64.b64decode(encrypted_auth)
decrypted_auth = kms.decrypt(CiphertextBlob = binary_auth)
auth = decrypted_auth['Plaintext'].decode()

header = {}
header['Authorization'] = auth
header['Accept'] = 'application/json'

url = 'https://snowflake.jamfcloud.com/JSSResource'

def lambda_handler(event, context):
    
    id_request = requests.get(url+ '/computergroups', headers=header)

    computers = json.loads(id_request.text)['computer_groups']
    
    reports = {}
    reports['timestamp'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for computer in computers:
        reports_request = requests.get(url+'/computergroups/id/'+str(computer['id']), headers=header)
        output = json.loads(reports_request.text)
        reports[computer['name']] = output
    
    AWS_BUCKET_NAME = 'sfc-sec-jamf-logs'
    s3 = boto3.client('s3')
    bucket = AWS_BUCKET_NAME
    path = str(uuid.uuid4().hex + '.json')
    
    body = json.dumps(reports).encode()
    
    s3.put_object(Body=body, Bucket=bucket, Key=path)
