import socket
import socks
import logging


def enable_proxy(host='localhost', port=10808):
    logging.log(logging.INFO, f'开启网络代理: socks5 {host}:{port}')
    socks.setdefaultproxy(socks.SOCKS5, host, port)
    socket.socket = socks.socksocket
