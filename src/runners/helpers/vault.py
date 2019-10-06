from os import environ
from base64 import b64decode, b64encode
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from .dbconfig import SA_KMS_REGION

KMS_KEY = environ.get('SA_KMS_KEY')
ENABLED = bool(KMS_KEY)

kms = boto3.client('kms', region_name=SA_KMS_REGION)


def decrypt_if_encrypted(
    ct: Optional[str] = None, envar: Optional[str] = None
) -> Optional[str]:
    if envar:
        ct = environ.get(envar)

    if not ct or len(ct) < 205:  # 1-byte plaintext has 205-byte ct
        return ct

    try:
        ctBlob = b64decode(ct)
    except Exception:
        ctBlob = ct.encode()

    try:
        res = None  # retry on incomplete response
        while res is None or 'Plaintext' not in res:
            res = kms.decrypt(CiphertextBlob=ctBlob)

        return res['Plaintext'].decode()

    except ClientError:
        raise

    except Exception:
        return ct


def encrypt(pt):
    return b64encode(
        kms.encrypt(KeyId=KMS_KEY, Plaintext=pt)['CiphertextBlob']
    ).decode()
