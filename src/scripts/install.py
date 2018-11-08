#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base64 import b64encode
from configparser import ConfigParser
import os
from getpass import getpass
import re
from typing import List, Optional, Tuple
from uuid import uuid4

import boto3
import snowflake.connector

from config import DATABASE, DATA_SCHEMA, RULES_SCHEMA, RESULTS_SCHEMA
from config import ALERTS_TABLE, VIOLATIONS_TABLE
from config import ALERT_QUERY_POSTFIX, ALERT_SQUELCH_POSTFIX
from config import VIOLATION_QUERY_POSTFIX, VIOLATION_SQUELCH_POSTFIX
from helpers import log

USER = "snowalert"
ROLE = "snowalert"
WAREHOUSE = "snowalert"

SCRIPT_DIR = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
ROOT_DIR = os.path.realpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
CREDS_DIR = ROOT_DIR

GRANT_PRIVILEGES_QUERIES = [
    f'GRANT ALL PRIVILEGES ON WAREHOUSE {WAREHOUSE} TO ROLE {ROLE};',
    f'GRANT ALL PRIVILEGES ON DATABASE {DATABASE} TO ROLE {ROLE};',
    f'GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE {DATABASE} TO ROLE {ROLE};',
    f'GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA {DATA_SCHEMA} TO ROLE {ROLE};',
    f'GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA {RULES_SCHEMA} TO ROLE {ROLE};',
    f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {RULES_SCHEMA} TO ROLE {ROLE};',
    f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {DATA_SCHEMA} TO ROLE {ROLE};',
    f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {RESULTS_SCHEMA} TO ROLE {ROLE};',
    f'GRANT USAGE ON WAREHOUSE {WAREHOUSE} TO ROLE {ROLE};',
]

WAREHOUSE_QUERIES = [
    f"""
      CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE}
        WAREHOUSE_SIZE=xsmall WAREHOUSE_TYPE=standard
        AUTO_SUSPEND=60 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE;
    """,
    f"ALTER USER {USER} SET default_warehouse={WAREHOUSE};"
]
CREATE_DATABASE_QUERY = f"CREATE DATABASE IF NOT EXISTS {DATABASE};"
CREATE_SCHEMAS_QUERIES = [
    f"CREATE SCHEMA IF NOT EXISTS {DATA_SCHEMA};",
    f"CREATE SCHEMA IF NOT EXISTS {RULES_SCHEMA};",
    f"CREATE SCHEMA IF NOT EXISTS {RESULTS_SCHEMA};",
]

CREATE_TABLES_QUERIES = [
    f"""
      CREATE TABLE IF NOT EXISTS {ALERTS_TABLE}(
        alert VARIANT
        , alert_time TIMESTAMP_LTZ(9)
        , event_time TIMESTAMP_LTZ(9)
        , ticket STRING
        , suppressed BOOLEAN
        , suppression_rule STRING DEFAULT NULL
        , counter INTEGER DEFAULT 1
      );
    """,
    f"""
      CREATE TABLE IF NOT EXISTS {VIOLATIONS_TABLE}(
        result VARIANT
        , alert_time TIMESTAMP_LTZ(9)
        , ticket STRING
        , suppressed BOOLEAN
        , suppression_rule STRING DEFAULT NULL
      );
    """
]

CREATE_TABLE_SUPPRESSIONS_QUERY = f"CREATE TABLE IF NOT EXISTS suppression_queries ( suppression_spec VARIANT );"

CREATE_SAMPLE_DATA_QUERY = f"""
  CREATE VIEW IF NOT EXISTS {DATA_SCHEMA}.successful_snowflake_logins_v AS
    SELECT *
    FROM TABLE(snowflake_sample_data.information_schema.login_history())
    WHERE is_success='YES'
  ;
"""

CREATE_SAMPLE_ALERT_QUERIES = [
    f"""
      CREATE VIEW IF NOT EXISTS {RULES_SCHEMA}.snowflake_login_without_mfa_{ALERT_QUERY_POSTFIX} AS
        SELECT 'Successful Snowflake login without MFA' AS title
             , array_construct('successful_snowflake_logins') AS sources
             , user_name AS object
             , 'SnowAlert' AS environment
             , event_timestamp AS event_time
             , CURRENT_TIMESTAMP() AS alert_time
             , reported_client_type || ' logging in as ' || user_name || ' from ' || client_ip AS description
             , user_name AS actor
             , 'login' AS action
             , 'SnowAlert' AS detector
             , OBJECT_CONSTRUCT(*) AS event_data
             , 'low' AS severity
             , 'snowflake_login_without_mfa' AS query_name
             , '{uuid4().hex}' AS query_id
          FROM {DATA_SCHEMA}.successful_snowflake_logins_v
          WHERE second_authentication_factor IS NULL
            AND DATEDIFF(MINUTE, event_timestamp, CURRENT_TIMESTAMP()) < 60
      ;
    """,
    f"""
      CREATE VIEW IF NOT EXISTS {RULES_SCHEMA}.snowflake_login_without_mfa_{ALERT_SQUELCH_POSTFIX} AS
        SELECT * FROM {ALERTS_TABLE}
        WHERE suppressed IS NULL
          AND alert:AffectedObject = 'DESIGNATED_NOMFA_USER'
    """,
]

CREATE_SAMPLE_VIOLATION_QUERIES = [
    f"""
      CREATE VIEW IF NOT EXISTS {RULES_SCHEMA}.no_violation_queries_in_too_long_{VIOLATION_QUERY_POSTFIX} AS
        SELECT 'SnowAlert' AS affectedenv
             , 'Violations' AS affectedobject
             , 'Cloudtrail' AS affectedobjecttype
             , 'No Violations Too Long' AS alerttype
             , CURRENT_TIMESTAMP() AS event_time
             , 'There have been no violations in the past 3 days.' AS description
             , 'SnowAlert' AS detector
             , 'low' AS severity
             , HASH(event_time::string || description::string) AS event_hash
             , 'no_violation_queries_in_too_long' AS event_def
        FROM (
          SELECT COUNT(*) AS count
          FROM snowalert.results.violations
          WHERE 1=1
            AND event_time > DATEADD('day', -3, CURRENT_DATE())
        )
        WHERE count=0
        ;
    """,
    f"""
      CREATE VIEW IF NOT EXISTS {RULES_SCHEMA}.no_violation_queries_in_too_long_{VIOLATION_SQUELCH_POSTFIX} AS
        SELECT * FROM {VIOLATIONS_TABLE}
        WHERE suppressed IS NULL
          AND 1=0
    """,
]


def login():
    config = ConfigParser()
    if config.read(os.path.expanduser('~/.snowsql/config')) and 'connections' in config:
        account = config['connections'].get('accountname')
        username = config['connections'].get('username')
        password = config['connections'].get('password')
        region = config['connections'].get('region')
    else:
        account = None
        username = None
        password = None
        region = None

    print("This is the installer for SnowAlert; it will set up all of the resources needed for SnowAlert to run.")

    if not account:
        account = input("Snowflake account where SnowAlert can store data, rules, and results: ")
    else:
        print(f"Loaded from ~/.snowcli/config: account '{account}'")

    print("Next, authenticate installer with user who has 'accountadmin' role in your Snowflake account")
    if not username:
        username = input("Snowflake username: ")
    else:
        print(f"Loaded from ~/.snowcli/config: username '{username}'")

    if not password:
        password = getpass("Password [leave blank for SSO for authentication]: ")
    else:
        print(f"Loaded from ~/.snowcli/config: password {'*' * len(password)}")

    region = region or input("Region of your Snowflake account [blank for us-west-2]: ")

    connect_kwargs = {'user': username, 'account': account}
    if password == '':
        connect_kwargs['authenticator'] = 'externalbrowser'
    else:
        connect_kwargs['password'] = password
    if region != '':
        connect_kwargs['region'] = region

    if 'region' in connect_kwargs and connect_kwargs['region'] == 'us-west-2':
        del connect_kwargs['region']

    def attempt(message="doing", todo=None):
        print(f"{message}", end="..", flush=True)
        try:
            if type(todo) is str:
                retval = ctx.cursor().execute(todo).fetchall()
                print('.', end='', flush=True)
            if type(todo) is list:
                retval = [ctx.cursor().execute(query) for query in todo if (True, print('.', end='', flush=True))]
            elif callable(todo):
                retval = todo()
        except Exception as e:
            log.fatal("failed", e)
        print(" ✓")
        return retval

    ctx = attempt("Authenticating to Snowflake", lambda: snowflake.connector.connect(**connect_kwargs))

    return ctx, account, region or "us-west-2", attempt


def load_aws_config() -> Tuple[str, str]:
    parser = ConfigParser()
    if parser.read(os.path.expanduser('~/.aws/config')) and 'default' in parser:
        c = parser['default']
        aws_key = c.get('aws_access_key_id')
        secret = c.get('aws_secret_access_key')
    else:
        return '', ''

    return aws_key, secret


def setup_warehouse(do_attempt):
    do_attempt("Creating and setting default warehouse", WAREHOUSE_QUERIES)
    do_attempt("Creating database", CREATE_DATABASE_QUERY)
    do_attempt("Creating schemas", CREATE_SCHEMAS_QUERIES)
    do_attempt("Creating alerts & violations tables", CREATE_TABLES_QUERIES)


def setup_user(do_attempt):
    defaults = f"login_name='{USER}' password='' default_role={ROLE} default_warehouse='{WAREHOUSE}'"
    do_attempt("Creating role and user", [
        f"CREATE ROLE IF NOT EXISTS {ROLE}",
        f"CREATE USER IF NOT EXISTS {USER} {defaults}",
        f"ALTER USER IF EXISTS {USER} SET {defaults}",  # in case user was manually created
    ])
    do_attempt("Granting role to user", f"GRANT ROLE {ROLE} TO USER {USER};")
    do_attempt("Granting privileges to role", GRANT_PRIVILEGES_QUERIES)


def setup_samples(ctx):
    do_attempt("Creating data view", CREATE_SAMPLE_DATA_QUERY)
    do_attempt("Creating sample alert", CREATE_SAMPLE_ALERT_QUERIES)
    do_attempt("Creating sample violation", CREATE_SAMPLE_VIOLATION_QUERIES)


def jira_integration():
    flag = input("Would you like to integrate Jira with SnowAlert (y/N)? ")
    if (flag.lower() in {'y', 'yes'}):
        jira_user = input("Please enter the username for the SnowAlert user in Jira: ")
        jira_password = getpass("Please enter the password for the SnowAlert user in Jira: ")
        jira_url = input("Please enter the URL for the Jira integration: ")
        if jira_url[:8] != "https://":
            jira_url = "https://" + jira_url
        print("Please enter the project tag for the alerts...")
        print("Note that this should be the text that will prepend the ticket id; if the project is SnowAlert")
        print("and the tickets will be SA-XXXX, then you should enter 'SA' for this prompt.")
        jira_project = input("Please enter the project tag for the alerts from SnowAlert: ")
        return jira_user, jira_password, jira_url, jira_project
    else:
        return "", "", "", ""


def genrsa(passwd: Optional[str] = None) -> Tuple[bytes, bytes]:
    from cryptography.hazmat.primitives import serialization as cs  # crypto serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.backends import default_backend as crypto_default_backend

    key = rsa.generate_private_key(
        backend=crypto_default_backend(),
        public_exponent=65537,
        key_size=2048
    )
    return (
        key.private_bytes(
            cs.Encoding.PEM,
            cs.PrivateFormat.PKCS8,
            encryption_algorithm=cs.BestAvailableEncryption(passwd.encode('utf-8')) if passwd else cs.NoEncryption()
        ),
        key.public_key().public_bytes(
            cs.Encoding.PEM,
            cs.PublicFormat.SubjectPublicKeyInfo
        )
    )


def setup_authentication(jira_password):
    print("The access key for SnowAlert's Snowflake account can have a passphrase, if you wish.")
    pk_passwd = getpass("RSA key passphrase [blank for none, '.' for random]: ")
    if pk_passwd == '.':
        pk_passwd = b64encode(os.urandom(18)).decode('utf-8')
        print("Generated random passphrase.")

    private_key, public_key = genrsa(pk_passwd)

    if pk_passwd:
        print("\nAdditionally, you may use Amazon Web Services for encryption and audit.")
        kms = boto3.client('kms', region_name=region)
        while True:
            try:
                pk_passwd, jira_password = do_kms_encrypt(kms, pk_passwd, jira_password)
                break

            except KeyboardInterrupt:
                log.fatal("User ended installation")

            except Exception as e:
                print(f"error {e!r}, trying.")

    rsa_public_key = re.sub(r'---.*---\n', '', public_key.decode('utf-8'))
    do_attempt("Setting auth key on snowalert user", f"ALTER USER {USER} SET rsa_public_key='{rsa_public_key}';")

    return private_key, pk_passwd, jira_password


def gen_envs(jira_user, jira_project, jira_url, jira_password, account, region, private_key, pk_passwd,
             aws_key, aws_secret, **x):
    vars = [
        f'SNOWFLAKE_ACCOUNT={account}',
        f'SA_USER={USER}',
        f'SA_WAREHOUSE={WAREHOUSE}',
        f'REGION={region or "us-west-2"}',

        f'PRIVATE_KEY={b64encode(private_key).decode("utf-8")}',
        f'PRIVATE_KEY_PASSWORD={pk_passwd}',
    ]

    if jira_url:
        vars += [
            f'JIRA_URL={jira_url}',
            f'JIRA_PROJECT={jira_project}',
            f'JIRA_USER={jira_user}',
            f'JIRA_PASSWORD={jira_password}',
        ]

    if aws_key:
        vars += [
            f'AWS_ACCESS_KEY_ID={aws_key}' if aws_key else '',
            f'AWS_SECRET_ACCESS_KEY={aws_secret}' if aws_secret else '',
        ]

    return '\n'.join(vars)


def do_kms_encrypt(kms, *args: str) -> List[str]:
    key = input("Enter IAM KMS KeyId or 'alias/{KeyAlias}' [blank for none, '.' for random]: ")

    if not key:
        return list(args)

    if key == '.':
        result = kms.create_key()
        key = result['KeyMetadata']['KeyId']

    return [
        b64encode(kms.encrypt(KeyId=key, Plaintext=s).get('CiphertextBlob')).decode('utf-8') if s else ""
        for s in args
    ]


if __name__ == '__main__':
    ctx, account, region, do_attempt = login()

    do_attempt("Use role accountadmin", "USE ROLE accountadmin")
    setup_warehouse(do_attempt)
    setup_samples(do_attempt)
    setup_user(do_attempt)

    jira_user, jira_password, jira_url, jira_project = jira_integration()

    print(f"\n--- DB setup complete! Now, let's prep the runners... ---\n")

    private_key, pk_passwd, jira_password = setup_authentication(jira_password)
    aws_key, aws_secret = load_aws_config()

    print(
        f"\n--- ...all done! Next, run... ---\n"
        f"\ncat <<END_OF_FILE > snowalert-{account}.envs\n{gen_envs(**locals())}\nEND_OF_FILE\n"
        f"\n### ...and then... ###\n"
        f"\ndocker run --env-file snowalert-{account}.envs snowsec/snowalert ./run\n"
        f"\n--- ...the end. ---\n"
    )
