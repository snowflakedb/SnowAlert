from os import environ
from base64 import b64decode, b64encode
from typing import Optional

import boto3
from botocore.exceptions import ClientError, HTTPClientError

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
            n = 10
            try:
                res = kms.decrypt(CiphertextBlob=ctBlob)
            except HTTPClientError:
                # An HTTP Client raised and unhandled exception:
                # [(
                #     'SSL routines',
                #     'ssl3_get_record',
                #     'decryption failed or bad record mac',
                # )]
                # fixed by waiting
                import time
                time.sleep(0.1)
                n -= 1
                if n == 0:
                    raise

        return res['Plaintext'].decode()

    except ClientError:
        raise

    except Exception:
        return ct


def encrypt(pt):
    return b64encode(
        kms.encrypt(KeyId=KMS_KEY, Plaintext=pt)['CiphertextBlob']
    ).decode()
