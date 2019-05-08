from base64 import b64decode

import boto3

from .dbconfig import REGION

kms = boto3.client('kms', region_name=REGION)


def decrypt_if_encrypted(ct: str) -> str:
    if not ct or len(ct) < 205:  # 1-byte plaintext has 205-byte ct
        return ct

    try:
        ctBlob = b64decode(ct)
    except Exception:
        ctBlob = ct

    try:
        res = None  # retry on incomplete response
        while res is None or 'Plaintext' not in res:
            res = kms.decrypt(CiphertextBlob=ctBlob)

        return res['Plaintext'].decode()

    except Exception:
        return ct
