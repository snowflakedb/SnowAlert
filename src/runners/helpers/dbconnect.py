import socket
from unittest.mock import patch

from snowflake.connector.connection import SnowflakeConnection as BadSnowflakeConnection


def newer_open_new(url):
    print(f'--- Click below ---\n{url}\n- - - - - - - - - -', flush=True)
    return True


class BetterSocket(socket.socket):
    def __init__(self, *args, **kwargs):
        return super(BetterSocket, self).__init__(*args, **kwargs)

    def close(self, *args, **kwargs):
        super(BetterSocket, self).close(*args, **kwargs)

    def bind(self, address):
        return super(BetterSocket, self).bind(('0.0.0.0', 1901))


@patch('socket.socket', new=BetterSocket)
@patch('webbrowser.open_new', new=newer_open_new)
def snowflake_connect(**kwargs):
    return BadSnowflakeConnection(**kwargs)
