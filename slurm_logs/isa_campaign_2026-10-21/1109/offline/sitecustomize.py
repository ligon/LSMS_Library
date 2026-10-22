"""Private verification only: refuse outbound IP connections in all Python children."""
import socket
_original = socket.socket.connect

def _offline_connect(sock, address):
    if sock.family in (socket.AF_INET, socket.AF_INET6):
        raise RuntimeError('Network disabled for private GH1109 source-cache verification')
    return _original(sock, address)

socket.socket.connect = _offline_connect
