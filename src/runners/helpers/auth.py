"""Helpers specific for SnowAlert, dealing with authentication, e.g. to Snowflake DB.
"""
from typing import Optional
from os import environ

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends.openssl.rsa import _RSAPrivateKey
from requests import post
from requests.auth import HTTPBasicAuth

from .vault import decrypt_if_encrypted


def load_pkb_rsa(p8_private_key: bytes, passphrase: Optional[bytes]) -> _RSAPrivateKey:
    """Loads the rsa private key instead of just the bytes, using password
    decrypted with KMS. Required for the snowpipe SimpleIngestManager
    authentication flow.
    """
    ptpass = decrypt_if_encrypted(passphrase.decode()) if passphrase else None
    private_key = serialization.load_pem_private_key(
        p8_private_key,
        password=ptpass.encode() if ptpass else None,
        backend=default_backend(),
    )
    return private_key


def oauth_refresh(account: str, refresh_token: str) -> str:
    OAUTH_CLIENT_ID = environ.get(
        f'OAUTH_CLIENT_{account.partition(".")[0].upper()}', ''
    )
    OAUTH_SECRET_ID = environ.get(
        f'OAUTH_SECRET_{account.partition(".")[0].upper()}', ''
    )

    return (
        post(
            f'https://{account}.snowflakecomputing.com/oauth/token-request',
            auth=HTTPBasicAuth(OAUTH_CLIENT_ID, OAUTH_SECRET_ID),
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data={'grant_type': 'refresh_token', 'refresh_token': refresh_token},
        )
        .json()
        .get('access_token')
    )


def load_pkb(p8_private_key: bytes, passphrase: Optional[bytes]) -> bytes:
    """Loads private key bytes out of p8-encoded private key, using password
    decrypted via KMS, e.g.:

      > pkp8 = open('rsa_key.p8').read().encode('ascii')
      > encrypted_pass = base64.b64decode(open('passphrase').read())
      > pkb = load_pkb(pkp8, encrypted_pass)
      > len(pkb) > 1000
      True

    """

    return load_pkb_rsa(p8_private_key, passphrase).private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
