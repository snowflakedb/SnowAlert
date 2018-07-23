import os
import base64
from unittest import mock
from unittest.mock import patch
import boto3
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from nose.tools import (
    assert_equal,
    raises
)

import botocore.session
from botocore.stub import Stubber

from adapters.AwsLambda.main import AwsLambda

kms = boto3.client('kms', region_name='us-west-2')

# Private key used for testing, not sensitive. The key's password is pk_password.
fake_private_key = (base64.b64encode(b"""-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIE6TAbBgkqhkiG9w0BBQMwDgQIRJSRMHKebS4CAggABIIEyHopr6z6QF9kAH3y
ok7Ejyj9TgS9WXCJt2n0e7yhM/TsN1HoOUzfocchjZQuYZMRixi6xRyQRSifngB4
It+5/1hC+Vf3jYUgUZCp1fv65/2KwH4gbrXpXCZpZ6wdtcOEU8YcQ6wfCXPDVjq+
ZnJ7a4Rh9JYrvjRovXOjDGnJs56uIT0yjCuMOM0W9FpBpZOpteba5ZUSTfN/bYDB
9HIxmQxHTr7XuzYOgO66HqrllA9DmvHzfY24ApkrvqN0yKj71aIRt3UrnubLzf3B
GsVRYY4njg+DVq75Q1yAlYLVk8L7Avs/+ELugHQn3WGJNANB7SRSJP86S1rOqfZo
akl0tNqe+uSquqwOiK2+5IIBVl5nkjjhR+EDeLh4QgZlIQe7v7cQox7TGRfrnya6
/CVVtvxi+y49UC0j4Co7z9RI3Iuck0njKf431EUQxwdkMEuAuT7YSnB7jmmfR0nE
T9kBDNQHJMClKA8ziURIlO83mBHf2fJe+tKuqjAuj6ZQ/yNnyBfyUePdbez+R5hP
EBoe/VlFAt3fJvXLu3AKbUIuau4U1ElrK16+GHciDMcY3h7zkf/JJ0+lAX9BOe9G
W6TBFsOEex1rvkMMEiIZlgV9PeBSxfzVt9Nsb4SvKyoOZFr2eXCundT9fLsYu46B
iL8Xt1jacnyoaWoV+PteyXqSva8gZtpGRH3XW/nrPt57eaUIp04+N37Cg1h1nywJ
gtdxT0HZPGktd7Yl04zSzOtX3SgTQ5mvYWJ7nrzsXmb9sRZ087uWcg2iwNkEL0eD
YHHTiX8MRxZEzVNa93HsRPPeEkoKot8PZMzWYXMUSLGZMTe9Pl/Bu7tI95K/CEB/
dQgW+MPK5/rjt1X1dqqeAnj5CQMaWcM7zc+6D8EWhpj3NMztjF4OcXbiOndAHXd3
nmwzfGx4bg90PSV/g+W8OONvhuJrVtNBbmHwBJU7fjIZEBMCKJizjgfbWeMdvw0g
YnQ1pGffEcvo4vZ0lkfdeDV/rR3iXoGMZ7V0aD4wvw358FzDyhGCKwN/55W81kha
T26kseQSLqnBgV5iDL7eRxO9QikDMwsn2XnxSiVdKoUw3fVM7SLhmtuEGDlhjvq7
vTFhxb2+WeoA/fJ+j4f2g3TKiAPPRGd78bp66ulBBGB989+8W2021ZIGcvFAQH5g
0gAzo2hCdBL81EjEBLh9FvEpxW41f5sRFziMbQAq83v4BieToMBB8y5Pa78PKuQU
7jqXzsUix/nJNRjBwrSVHqDZyU2TIAt/ZyyvWbtevx8WFsSz7I8aOpmDygrWPnqM
go6qF63wdwDMkN2cud2YIrKrcX0mfOD99ylJ4m2Z630J9pECbJ+/O5tKze53HbW5
nCvtgajjjrJyiorySGtdUWxPh5/3q8sDrc6gxxahgOvZeUxVDYIXbobvgzbVj2is
igbfuGdy7jsiTt/N62AiFJuhJtniTJf+DkGtwdP6jFW39/JRX12LYorVXW3tZhNg
Qsys/4HQGDGDIzV8bFqyfA+vY4IUOEbSfF3phQYXqJQ6C1lNqGQRKBdAXweiCrha
fQlccmhuIpQcF2gqHDgiT3m/3vCnvvzBJzn37J4TZ/+MmxaeWRufVebozNooG+bU
s1wXoes7N9C4oEfAAA==
-----END ENCRYPTED PRIVATE KEY-----""")).decode("utf-8")


def get_test_key_bytes():
    test_key = serialization.load_pem_private_key(base64.b64decode(fake_private_key),
                                                  password=b'swordfish', backend=default_backend())
    return test_key.private_bytes(encoding=serialization.Encoding.DER,
                                  format=serialization.PrivateFormat.TraditionalOpenSSL,
                                  encryption_algorithm=serialization.NoEncryption())


class TestAwsLambda(object):

    @mock.patch.dict(os.environ, {'private_key_password': 'cGtfcGFzc3dvcmQ='})
    @mock.patch.dict(os.environ, {'private_key': fake_private_key})
    @patch('boto3.client')
    def test_load_api_creds(self, mock_boto_call):
        mock_kms = botocore.session.get_session().create_client('kms')
        response = {'KeyId': '0',
                    'Plaintext': b'swordfish'
                    }
        expected_params = {'CiphertextBlob': b'pk_password'}

        stubber = Stubber(mock_kms)
        stubber.add_response('decrypt', response, expected_params)
        mock_boto_call.return_value = mock_kms
        stubber.activate()

        test_lambda = AwsLambda()
        result = test_lambda.get_key()

        assert_equal(result, get_test_key_bytes())


t = TestAwsLambda
t.test_load_api_creds(t)
