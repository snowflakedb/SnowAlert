"""Helper specific to SnowAlert connecting to the database"""

from base64 import b64decode
import os
from typing import List
import socket

import snowflake.connector

from runners.config import USER, REGION
from . import log
from .auth import load_pkb

CACHED_CONNECTION = None


def retry(f, E=Exception, n=3):
    while 1:
        try:
            return f()
        except E:
            n -= 1
            if n < 0:
                raise


###
# Connecting
###

class WebbrowserPkg(object):
    @staticmethod
    def open_new(url):
        print(f'auth as {USER} here', url, flush=True)
        return True


class SocketPkg(socket.socket):
    def __init__(self, *args, **kwargs):
        # print('init socketpkg', flush=True)
        return super(SocketPkg, self).__init__(*args, **kwargs)

    def close(self, *args, **kwargs):
        # print('close socketpkg', flush=True)
        super(SocketPkg, self).close(*args, **kwargs)

    def bind(self, address):
        # print(f'bind to 0.0.0.0:1901 instead of {address}', flush=True)
        return super(SocketPkg, self).bind(('0.0.0.0', 1901))


class SnowflakeConnection(snowflake.connector.connection.SnowflakeConnection):
    def __open_connection(self):
        u"""
        Opens a new network connection
        """
        self.converter = self._converter_class(
            use_sfbinaryformat=False,
            use_numpy=self._numpy)

        self._rest = snowflake.connector.network.SnowflakeRestful(
            host=self.host,
            port=self.port,
            proxy_host=self.proxy_host,
            proxy_port=self.proxy_port,
            proxy_user=self.proxy_user,
            proxy_password=self.proxy_password,
            protocol=self._protocol,
            inject_client_pause=self._inject_client_pause,
            connection=self)

        if self.host.endswith(u".privatelink.snowflakecomputing.com"):
            ocsp_cache_server = \
                u'http://ocsp{}/ocsp_response_cache.json'.format(
                    self.host[self.host.index('.'):])
            os.environ['SF_OCSP_RESPONSE_CACHE_SERVER_URL'] = ocsp_cache_server
        else:
            if 'SF_OCSP_RESPONSE_CACHE_SERVER_URL' in os.environ:
                del os.environ['SF_OCSP_RESPONSE_CACHE_SERVER_URL']

        auth_instance = snowflake.connector.auth_webbrowser.AuthByWebBrowser(
            self.rest, self.application, protocol=self._protocol,
            host=self.host, port=self.port, webbrowser_pkg=WebbrowserPkg, socket_pkg=SocketPkg)

        if self._session_parameters is None:
            self._session_parameters = {}
        if self._autocommit is not None:
            self._session_parameters['AUTOCOMMIT'] = self._autocommit

        if self._timezone is not None:
            self._session_parameters['TIMEZONE'] = self._timezone

        if self.client_session_keep_alive:
            self._session_parameters['CLIENT_SESSION_KEEP_ALIVE'] = True

        # enable storing temporary credential in a file
        self._session_parameters['CLIENT_STORE_TEMPORARY_CREDENTIAL'] = True

        auth = snowflake.connector.auth.Auth(self.rest)
        if not auth.read_temporary_credential(self.account, self.user, self._session_parameters):
            self.__authenticate(auth_instance)
        else:
            # set the current objects as the session is derived from the id
            # token, and the current objects may be different.
            self._set_current_objects()

        self._password = None  # ensure password won't persist

        if self.client_session_keep_alive:
            self._add_heartbeat()


def preflight_checks(ctx):
    user_props = dict((x['property'], x['value']) for x in fetch(ctx, f'DESC USER {USER};'))
    assert user_props.get('DEFAULT_ROLE') != 'null', f"default role on user {USER} must not be null"


def connect(run_preflight_checks=True):
    global CACHED_CONNECTION
    if CACHED_CONNECTION:
        return CACHED_CONNECTION

    if 'PRIVATE_KEY' in os.environ:
        p8_private_key = b64decode(os.environ['PRIVATE_KEY'])
        encrypted_pass = (os.environ['PRIVATE_KEY_PASSWORD']).encode('utf-8')
        snowflake_connect = snowflake.connector.connection.SnowflakeConnection
        authenticator = None
    else:
        snowflake_connect = SnowflakeConnection
        authenticator = 'EXTERNALBROWSER'

    user = USER
    region = REGION
    account = os.environ['SNOWFLAKE_ACCOUNT'] + ('' if region == 'us-west-2' else f'.{region}')

    try:
        connection = snowflake_connect(
            user=user,
            account=account,
            private_key=None if authenticator else load_pkb(p8_private_key, encrypted_pass),
            authenticator=authenticator,
            ocsp_response_cache_filename='/tmp/.cache/snowflake/ocsp_response_cache'
        )

        if run_preflight_checks:
            preflight_checks(connection)

    except Exception as e:
        log.fatal(e, "Failed to connect.")

    CACHED_CONNECTION = connection
    return connection


def fetch(ctx, query):
    res = execute(ctx, query)
    cols = [c[0] for c in res.description]
    while True:
        row = res.fetchone()
        if row is None:
            break
        yield dict(zip(cols, row))


def execute(ctx, query):
    try:
        return ctx.cursor().execute(query)
    except snowflake.connector.errors.ProgrammingError as e:
        log.error(e, f"Programming Error in query: {query}")
        return ctx.cursor().execute("SELECT 1 FROM FALSE;")


def connect_and_execute(queries=None):
    connection = connect()

    if type(queries) is str:
        execute(connection, queries)

    if type(queries) is list:
        for q in queries:
            execute(connection, q)

    return connection


def connect_and_fetchall(query):
    ctx = connect()
    return ctx, execute(ctx, query).fetchall()


###
# Looking at SnowAlert specific things
###

def load_rules(ctx, postfix) -> List[str]:
    from config import RULES_SCHEMA
    try:
        views = ctx.cursor().execute(f'SHOW VIEWS IN {RULES_SCHEMA}').fetchall()
    except Exception as e:
        log.fatal(e, f"Loading '{postfix}' rules failed.")

    rules = [name[1] for name in views if name[1].lower().endswith(postfix)]
    log.info(f"Loaded {len(views)} views, {len(rules)} were '{postfix}' rules.")

    return rules
