from base64 import b64decode

import boto3

from .dbconfig import REGION

kms = boto3.client('kms', region_name=REGION)


def decrypt_if_encrypted(ct, handleErrors=True):
    try:
        ctb = b64decode(ct)
    except Exception:
        ctb = ct

    try:
        return kms.decrypt(CiphertextBlob=ctb)['Plaintext'].decode()
    except Exception:
        if handleErrors:
            return ct
        else:
            raise
