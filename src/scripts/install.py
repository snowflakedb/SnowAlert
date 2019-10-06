#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""Script which installs the Snowflake database, warehouse, and everything
else you need to get started with SnowAlert.

Usage:

  ./install.py [--admin-role ADMIN_ROLE]

Note that if you run with ADMIN_ROLE other than ACCOUNTADMIN, the installer
assumes that your account admin has already created a user, role, database,
and warehouse for SnowAlert to use. The role will be the SnowAlert admin role,
and will be used by those managing the rules, separate from the SNOWALERT role
which will be used by the runners.
"""

from base64 import b64encode
from configparser import ConfigParser
import fire
from getpass import getpass
from os import environ, path, urandom
import re
from typing import List, Optional, Tuple
from urllib.parse import urlsplit
from uuid import uuid4

import boto3

from runners.config import ALERT_QUERY_POSTFIX, ALERT_SQUELCH_POSTFIX
from runners.config import VIOLATION_QUERY_POSTFIX
from runners.config import DATABASE, DATA_SCHEMA, RULES_SCHEMA, RESULTS_SCHEMA

from runners.helpers import log
from runners.helpers.dbconfig import USER, ROLE, WAREHOUSE
from runners.helpers.dbconnect import snowflake_connect


def read_queries(file, tmpl_vars=None):
    if tmpl_vars is None:
        tmpl_vars = {}

    tmpl_vars.update(
        {
            'uuid': uuid4().hex,
            'DATABASE': DATABASE,
            'DATA_SCHEMA': DATA_SCHEMA,
            'RULES_SCHEMA': RULES_SCHEMA,
            'ALERT_QUERY_POSTFIX': ALERT_QUERY_POSTFIX,
            'ALERT_SQUELCH_POSTFIX': ALERT_SQUELCH_POSTFIX,
            'VIOLATION_QUERY_POSTFIX': VIOLATION_QUERY_POSTFIX,
        }
    )

    pwd = path.dirname(path.realpath(__file__))
    tmpl = open(f'{pwd}/installer-queries/{file}.sql.fmt').read()
    return [t + ';' for t in tmpl.format(**tmpl_vars).split(';') if t.strip()]


VERBOSE = False

GRANT_OBJECT_PRIVILEGES_QUERIES = [
    # account
    f'GRANT EXECUTE TASK ON ACCOUNT TO ROLE {ROLE}',
    # data
    f'GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE {DATABASE} TO ROLE {ROLE}',
    f'GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA {DATA_SCHEMA} TO ROLE {ROLE}',
    f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {DATA_SCHEMA} TO ROLE {ROLE}',
    f'GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {DATA_SCHEMA} TO ROLE {ROLE}',
    # rules
    f'GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA {RULES_SCHEMA} TO ROLE {ROLE}',
    f'GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {RULES_SCHEMA} TO ROLE {ROLE}',
    # results
    f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {RESULTS_SCHEMA} TO ROLE {ROLE}',
]

WAREHOUSE_QUERIES = [
    f"""
      CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE}
        WAREHOUSE_SIZE=XSMALL
        WAREHOUSE_TYPE=STANDARD
        AUTO_SUSPEND=60
        AUTO_RESUME=TRUE
        INITIALLY_SUSPENDED=TRUE
    """
]
DATABASE_QUERIES = [f'CREATE DATABASE IF NOT EXISTS {DATABASE}']
GRANT_ACCOUNT_PRIVILEGES_TO_ROLE = [
    f'GRANT ALL PRIVILEGES ON DATABASE {DATABASE} TO ROLE {ROLE}',
    f'GRANT ALL PRIVILEGES ON WAREHOUSE {WAREHOUSE} TO ROLE {ROLE}',
]

CREATE_SCHEMAS_QUERIES = [
    f"CREATE SCHEMA IF NOT EXISTS data",
    f"CREATE SCHEMA IF NOT EXISTS rules",
    f"CREATE SCHEMA IF NOT EXISTS results",
]

CREATE_TABLES_QUERIES = [
    f"""
      CREATE TABLE IF NOT EXISTS results.alerts(
        alert VARIANT
        , alert_time TIMESTAMP_LTZ(9)
        , event_time TIMESTAMP_LTZ(9)
        , ticket STRING
        , suppressed BOOLEAN
        , suppression_rule STRING DEFAULT NULL
        , counter INTEGER DEFAULT 1
        , correlation_id STRING
        , handled VARIANT
      );
    """,
    f"""
      CREATE TABLE IF NOT EXISTS results.violations(
        result VARIANT
        , id STRING
        , alert_time TIMESTAMP_LTZ(9)
        , ticket STRING
        , suppressed BOOLEAN
        , suppression_rule STRING DEFAULT NULL
      );
    """,
    f"""
      CREATE TABLE IF NOT EXISTS results.query_metadata(
          event_time TIMESTAMP_LTZ
          , v VARIANT
          );
      """,
    f"""
      CREATE TABLE IF NOT EXISTS results.run_metadata(
          event_time TIMESTAMP_LTZ
          , v VARIANT
          );
      """,
    f"""
      CREATE TABLE IF NOT EXISTS results.ingestion_metadata(
          event_time TIMESTAMP_LTZ
          , v VARIANT
          );
    """,
]


def parse_snowflake_url(url):
    account = None
    region = None
    res = urlsplit(url)
    path = res.netloc or res.path

    c = path.split('.')

    if len(c) == 1:
        account = c[0]
    else:
        if c[-2:] == ['snowflakecomputing', 'com']:
            c.pop(-1)
            c.pop(-1)

        account = c.pop(0)
        region = '.'.join(c) if len(c) > 0 else 'us-west-2'

    return account, region


def login(configuration=None):
    config_file = '~/.snowsql/config'

    config = None
    if type(configuration) is dict:
        config = configuration
    if type(configuration) is str:
        parser = ConfigParser()
        config_section = (
            f'connections.{configuration}' if configuration else 'connections'
        )
        if parser.read(path.expanduser(config_file)) and config_section in parser:
            config = parser[config_section]

    if config is not None:
        account = config.get('accountname')
        username = config.get('username')
        password = config.get('password')
        region = config.get('region')
    else:
        account = None
        username = None
        password = None
        region = None

    print("Starting installer for SnowAlert.")

    if not account:
        while 1:
            url = input(
                "Snowflake account where SnowAlert can store data, rules, and results (URL or account name): "
            )
            account, region = parse_snowflake_url(url)
            if not account:
                print(
                    "That's not a valid URL for a snowflake account. Please check for typos and try again."
                )
            else:
                break
    else:
        print(f"Loaded account: '{account}'")

    if not region:
        region = input("Region of your Snowflake account [blank for us-west-2]: ")

    if not username:
        print("Next, authenticate installer --")
        username = input("Snowflake username: ")
    else:
        print(f"Loaded username: '{username}'")

    if not password:
        password = getpass("Password [leave blank for SSO for authentication]: ")
    else:
        print(f"Loaded password: {'*' * len(password)}")

    connect_kwargs = {'user': username, 'account': account}
    if password == '':
        connect_kwargs['authenticator'] = 'externalbrowser'
    else:
        connect_kwargs['password'] = password
    if region != '':
        connect_kwargs['region'] = region

    def attempt(message="doing", todo=None, fail_silently=False):
        print(f"{message}", end="..", flush=True)
        try:
            if type(todo) is str:
                retval = ctx.cursor().execute(todo).fetchall()
                print('.', end='', flush=True)
            if type(todo) is list:
                retval = [
                    ctx.cursor().execute(query)
                    for query in todo
                    if (True, print('.', end='', flush=True))
                ]
            elif callable(todo):
                retval = todo()

        except Exception:
            if fail_silently:
                return []
            raise

        print(" ✓")
        return retval

    ctx = attempt(
        "Authenticating to Snowflake", lambda: snowflake_connect(**connect_kwargs)
    )

    return ctx, account, region or "us-west-2", attempt


def load_aws_config() -> Tuple[str, str]:
    parser = ConfigParser()
    if parser.read(path.expanduser('~/.aws/config')) and 'default' in parser:
        c = parser['default']
        aws_key = c.get('aws_access_key_id')
        secret = c.get('aws_secret_access_key')
    else:
        return '', ''

    return aws_key, secret


def setup_warehouse_and_db(do_attempt):
    do_attempt("Creating warehouse", WAREHOUSE_QUERIES)
    do_attempt("Creating database", DATABASE_QUERIES)


def setup_schemas_and_tables(do_attempt, database):
    do_attempt(f"Use database {database}", f'USE DATABASE {database}')
    do_attempt("Creating schemas", CREATE_SCHEMAS_QUERIES)
    do_attempt("Creating alerts & violations tables", CREATE_TABLES_QUERIES)
    do_attempt("Creating standard UDTFs", read_queries('create-udtfs'))
    do_attempt("Creating standard data views", read_queries('data-views'))


def setup_user_and_role(do_attempt):
    defaults = f"login_name='{USER}' password='' default_role={ROLE} default_warehouse='{WAREHOUSE}'"
    do_attempt(
        "Creating role and user",
        [
            f"CREATE ROLE IF NOT EXISTS {ROLE}",
            f"CREATE USER IF NOT EXISTS {USER} {defaults}",
            f"ALTER USER IF EXISTS {USER} SET {defaults}",  # in case user was manually created
        ],
    )
    do_attempt("Granting role to user", f"GRANT ROLE {ROLE} TO USER {USER}")
    do_attempt(
        "Granting account level privileges to SA role", GRANT_ACCOUNT_PRIVILEGES_TO_ROLE
    )


def find_share_db_name(do_attempt):
    sample_data_share_rows = do_attempt(
        "Retrieving sample data share(s)",
        r"SHOW TERSE SHARES LIKE '%SAMPLE_DATA'",
        fail_silently=True,
    )

    # Database name is 4th attribute in row
    share_db_names = [share_row[3] for share_row in sample_data_share_rows]
    if len(share_db_names) == 0:
        VERBOSE and print(f"Unable to locate sample data share.")
        return

    # Prioritize potential tie-breaks
    for name in ('SNOWFLAKE_SAMPLE_DATA', 'SF_SAMPLE_DATA', 'SAMPLE_DATA'):
        if name in share_db_names:
            return name

    return share_db_names[0]


def setup_samples(do_attempt, share_db_name):
    tmpl_var = {"SNOWFLAKE_SAMPLE_DATA": share_db_name}
    VERBOSE and print(f"Using SAMPLE DATA share with name {share_db_name}")
    do_attempt(
        "Creating sample data view", read_queries('sample-data-queries', tmpl_var)
    )

    do_attempt("Creating sample alert", read_queries('sample-alert-queries'))
    do_attempt("Creating sample violation", read_queries('sample-violation-queries'))


def jira_integration(setup_jira=None):
    while setup_jira is None:
        uinput = input(
            "Would you like to integrate Jira with SnowAlert (y/N)? "
        ).lower()
        answered_yes = uinput.startswith('y')
        answered_no = uinput == '' or uinput.startswith('n')
        setup_jira = True if answered_yes else False if answered_no else None

    if setup_jira:
        jira_url = input("Please enter the URL for the Jira integration: ")
        if jira_url[:8] != "https://":
            jira_url = "https://" + jira_url
        jira_user = input("Please enter the username for the SnowAlert user in Jira: ")
        jira_password = getpass(
            "Please enter the password for the SnowAlert user in Jira: "
        )
        print("Please enter the project tag for the alerts...")
        print(
            "Note that this should be the text that will prepend the ticket id; if the project is SnowAlert"
        )
        print(
            "and the tickets will be SA-XXXX, then you should enter 'SA' for this prompt."
        )
        jira_project = input(
            "Please enter the project tag for the alerts from SnowAlert: "
        )
        return jira_user, jira_password, jira_url, jira_project
    else:
        return "", "", "", ""


def genrsa(passwd: Optional[str] = None) -> Tuple[bytes, bytes]:
    from cryptography.hazmat.primitives import (
        serialization as cs,
    )  # crypto serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.backends import default_backend as crypto_default_backend

    key = rsa.generate_private_key(
        backend=crypto_default_backend(), public_exponent=65537, key_size=2048
    )
    return (
        key.private_bytes(
            cs.Encoding.PEM,
            cs.PrivateFormat.PKCS8,
            encryption_algorithm=cs.BestAvailableEncryption(passwd.encode('utf-8'))
            if passwd
            else cs.NoEncryption(),
        ),
        key.public_key().public_bytes(
            cs.Encoding.PEM, cs.PublicFormat.SubjectPublicKeyInfo
        ),
    )


def setup_authentication(jira_password, region, pk_passphrase=None):
    print(
        "The access key for SnowAlert's Snowflake account can have a passphrase, if you wish."
    )

    if pk_passphrase is None:
        pk_passphrase = getpass("RSA key passphrase [blank for none, '.' for random]: ")

    if pk_passphrase == '.':
        pk_passphrase = b64encode(urandom(18)).decode('utf-8')
        print("Generated random passphrase.")

    private_key, public_key = genrsa(pk_passphrase)

    if pk_passphrase:
        print(
            "\nAdditionally, you may use Amazon Web Services for encryption and audit."
        )
        kms = boto3.client('kms', region_name=region)
        while True:
            try:
                pk_passphrase, jira_password = do_kms_encrypt(
                    kms, pk_passphrase, jira_password
                )
                break

            except KeyboardInterrupt:
                log.fatal("User ended installation")

            except Exception as e:
                print(f"error {e!r}, trying.")

    rsa_public_key = re.sub(r'---.*---\n', '', public_key.decode('utf-8'))

    return private_key, pk_passphrase, jira_password, rsa_public_key


def gen_envs(
    jira_user,
    jira_project,
    jira_url,
    jira_password,
    account,
    region,
    private_key,
    pk_passphrase,
    aws_key,
    aws_secret,
    **x,
):
    vars = [
        ('SNOWFLAKE_ACCOUNT', account),
        ('SA_USER', USER),
        ('SA_ROLE', ROLE),
        ('SA_DATABASE', DATABASE),
        ('SA_WAREHOUSE', WAREHOUSE),
        ('REGION', region or 'us-west-2'),
        ('PRIVATE_KEY', b64encode(private_key).decode("utf-8")),
        ('PRIVATE_KEY_PASSWORD', pk_passphrase.replace('$', r'\$')),
    ]

    if jira_url:
        vars += [
            ('JIRA_URL', jira_url),
            ('JIRA_PROJECT', jira_project),
            ('JIRA_USER', jira_user),
            ('JIRA_PASSWORD', jira_password),
        ]

    if aws_key:
        vars += [
            ('AWS_ACCESS_KEY_ID', aws_key if aws_key else ''),
            ('AWS_SECRET_ACCESS_KEY', aws_secret if aws_secret else ''),
        ]

    return vars


def do_kms_encrypt(kms, *args: str) -> List[str]:
    key = input(
        "Enter IAM KMS KeyId or 'alias/{KeyAlias}' [blank for none, '.' for new]: "
    )

    if not key:
        return list(args)

    if key == '.':
        result = kms.create_key()
        key = result['KeyMetadata']['KeyId']

    return [
        b64encode(kms.encrypt(KeyId=key, Plaintext=s).get('CiphertextBlob')).decode(
            'utf-8'
        )
        if s
        else ""
        for s in args
    ]


def main(
    admin_role="accountadmin",
    samples=True,
    _samples=True,
    pk_passphrase=None,
    jira=None,
    config_account=None,
    config_region=None,
    config_username=None,
    config_password=None,
    connection_name=None,
    uninstall=False,
    set_env_vars=False,
    verbose=False,
):

    global VERBOSE
    VERBOSE = verbose

    samples = samples and _samples  # so that --no-samples works, as well

    configuration = (
        connection_name
        if connection_name is not None
        else {
            'region': config_region or environ.get('REGION'),
            'accountname': config_account or environ.get('SNOWFLAKE_ACCOUNT'),
            'username': config_username or environ.get('SA_ADMIN_USER'),
            'password': config_password or environ.get('SA_ADMIN_PASSWORD'),
        }
    )

    ctx, account, region, do_attempt = login(configuration)

    if admin_role:
        do_attempt(f"Use role {admin_role}", f"USE ROLE {admin_role}")

    if uninstall:
        do_attempt(
            "Uninstalling",
            [
                f'DROP USER {USER}',
                f'DROP ROLE {ROLE}',
                f'DROP WAREHOUSE {WAREHOUSE}',
                f'DROP DATABASE {DATABASE}',
            ]
            if admin_role == 'accountadmin'
            else [
                f'DROP SCHEMA {DATA_SCHEMA}',
                f'DROP SCHEMA {RULES_SCHEMA}',
                f'DROP SCHEMA {RESULTS_SCHEMA}',
            ],
        )
        return

    if admin_role == "accountadmin":
        setup_warehouse_and_db(do_attempt)
        setup_user_and_role(do_attempt)

    setup_schemas_and_tables(do_attempt, DATABASE)

    if samples:
        share_db_name = find_share_db_name(do_attempt)
        if share_db_name:
            setup_samples(do_attempt, share_db_name)
        else:
            print("No share db found, skipping samples.")

    do_attempt(
        "Granting object level privileges to SA role", GRANT_OBJECT_PRIVILEGES_QUERIES
    )

    jira_user, jira_password, jira_url, jira_project = jira_integration(jira)

    print(f"\n--- DB setup complete! Now, let's prep the runners... ---\n")

    private_key, pk_passphrase, jira_password, rsa_public_key = setup_authentication(
        jira_password, region, pk_passphrase
    )

    if admin_role == "accountadmin":
        do_attempt(
            "Setting auth key on snowalert user",
            f"ALTER USER {USER} SET rsa_public_key='{rsa_public_key}'",
        )

    aws_key, aws_secret = load_aws_config()

    env_vars = gen_envs(**locals())
    if set_env_vars:
        for name, value in env_vars:
            environ[name] = value
    env_vars_str = '\n'.join([f'{n}={v}' for n, v in env_vars])
    print(
        f"\n--- ...all done! Next, run... ---\n"
        f"\ncat <<END_OF_FILE > snowalert-{account}.envs\n{env_vars_str}\nEND_OF_FILE\n"
        f"\n### ...and then... ###\n"
        f"\ndocker run --env-file snowalert-{account}.envs snowsec/snowalert ./run all\n"
        f"\n--- ...the end. ---\n"
    )


if __name__ == '__main__':
    fire.Fire(main)
