import os
import socket

from snowflake.connector.auth import Auth
from snowflake.connector.auth_webbrowser import AuthByWebBrowser
from snowflake.connector.connection import SnowflakeConnection as BadSnowflakeConnection
from snowflake.connector.network import SnowflakeRestful


def snowflake_connect(**kwargs):
    """The bad snowflake connection presumes too much! This one lets you override:
      1. how URL is displayed
      2. which port is listened on
    """

    class WebbrowserPkg(object):
        @staticmethod
        def open_new(url):
            print(f'auth here', url, flush=True)
            return True

    class SocketPkg(socket.socket):
        def __init__(self, *args, **kwargs):
            return super(SocketPkg, self).__init__(*args, **kwargs)

        def close(self, *args, **kwargs):
            super(SocketPkg, self).close(*args, **kwargs)

        def bind(self, address):
            return super(SocketPkg, self).bind(('0.0.0.0', 1901))

    class SnowflakeConnection(BadSnowflakeConnection):
        def __open_connection(self):
            u"""
            Opens a new network connection
            """
            self.converter = self._converter_class(
                use_sfbinaryformat=False,
                use_numpy=self._numpy)

            self._rest = SnowflakeRestful(
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

            auth_instance = AuthByWebBrowser(
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

            auth = Auth(self.rest)
            if not auth.read_temporary_credential(self.account, self.user, self._session_parameters):
                self.__authenticate(auth_instance)
            else:
                # set the current objects as the session is derived from the id
                # token, and the current objects may be different.
                self._set_current_objects()

            self._password = None  # ensure password won't persist

            if self.client_session_keep_alive:
                self._add_heartbeat()

    return SnowflakeConnection(**kwargs)
