import os
from base64 import b64decode
from typing import Optional

import boto3

from .dbconfig import REGION

kms = boto3.client('kms', region_name=REGION)


def decrypt_if_encrypted(ct: Optional[str] = None, envar: Optional[str] = None) -> Optional[str]:
    if envar:
        ct = os.environ.get(envar)

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

    except Exception:
        return ct
