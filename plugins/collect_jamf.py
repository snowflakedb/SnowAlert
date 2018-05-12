import boto3
import datetime
import json
import uuid
import os
import base64
from botocore.vendored import requests

JAMF_API_URL = 'https://<your-org>.jamfcloud.com/JSSResource'
AWS_BUCKET_NAME = 'jamf-logs'

kms = boto3.client('kms')
encrypted_auth = os.environ['auth']
binary_auth = base64.b64decode(encrypted_auth)
decrypted_auth = kms.decrypt(CiphertextBlob = binary_auth)
auth = decrypted_auth['Plaintext'].decode()

header = {}
header['Authorization'] = auth
header['Accept'] = 'application/json'


def lambda_handler(event, context):
    
    id_request = requests.get(JAMF_API_URL + '/computergroups', headers=header)

    computers = json.loads(id_request.text)['computer_groups']
    
    reports = {}
    reports['timestamp'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for computer in computers:
        reports_request = requests.get(url+'/computergroups/id/'+str(computer['id']), headers=header)
        output = json.loads(reports_request.text)
        reports[computer['name']] = output

    s3 = boto3.client('s3')
    path = str(uuid.uuid4().hex + '.json')
    body = json.dumps(reports).encode()
    s3.put_object(Body=body, Bucket=AWS_BUCKET_NAME, Key=path)
