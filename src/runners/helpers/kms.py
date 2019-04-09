from base64 import b64decode

import boto3

from .dbconfig import REGION

kms = boto3.client('kms', region_name=REGION)


def decrypt_if_encrypted(ct, handleErrors=True):
    if len(ct) < 32:
        # guess at smallest ciphertext
        return ct

    try:
        ctBlob = b64decode(ct)
    except Exception:
        ctBlob = ct

    try:
        # depending on local AWS config, this might ask for 2FA
        return kms.decrypt(CiphertextBlob=ctBlob)['Plaintext'].decode()
    except Exception:
        if handleErrors:
            return ct
        else:
            raise
