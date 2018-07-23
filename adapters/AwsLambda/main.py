import os
import base64
import boto3
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class AwsLambda(object):

    def get_key(self):
        kms = boto3.client('kms')
        password = kms.decrypt(CiphertextBlob=base64.b64decode(os.environ['private_key_password']))['Plaintext'].decode()
        private_key = serialization.load_pem_private_key(base64.b64decode(os.environ['private_key']),
                                                         password=password.encode(), backend=default_backend())
        pkb = private_key.private_bytes(encoding=serialization.Encoding.DER,
                                        format=serialization.PrivateFormat.TraditionalOpenSSL,
                                        encryption_algorithm=serialization.NoEncryption())
        return pkb

    # mock object #1: kms.decrypt returns 'swordfish'.encode()
    # mock object #2: os.environ['private key'] returns <known test key with 'swordfish' as password>.base64.b64encode()
    # assert that pkb = the private bytes of the known test key
