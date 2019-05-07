from base64 import b64decode

import boto3

from .dbconfig import REGION

kms = boto3.client('kms', region_name=REGION)


def decrypt_if_encrypted(ct, handleErrors=True, as_bytes=False) -> bytes:
    if not ct or len(ct) < 205:  # 1-byte plaintext has 205-byte ct
        return ct

    try:
        ctBlob = b64decode(ct)
    except Exception:
        ctBlob = ct

    try:
        # depending on local AWS config, this might ask for 2FA
        pt = kms.decrypt(CiphertextBlob=ctBlob)['Plaintext']
        return pt if as_bytes else pt.decode()
    except Exception:
        if handleErrors:
            return ct
        else:
            raise
