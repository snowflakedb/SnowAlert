import socket
from unittest.mock import patch

from snowflake.connector.connection import SnowflakeConnection as BadSnowflakeConnection


def open_new(url):
    print(f'--- Click below ---\n{url}\n- - - - - - - - - -', flush=True)
    return True


class SocketPkg(socket.socket):
    def __init__(self, *args, **kwargs):
        return super(SocketPkg, self).__init__(*args, **kwargs)

    def close(self, *args, **kwargs):
        super(SocketPkg, self).close(*args, **kwargs)

    def bind(self, address):
        return super(SocketPkg, self).bind(('0.0.0.0', 1901))


@patch('socket.socket', new=SocketPkg)
@patch('webbrowser.open_new', new=open_new)
def snowflake_connect(**kwargs):
    return BadSnowflakeConnection(**kwargs)
