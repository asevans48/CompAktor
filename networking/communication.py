import socket
import ssl

from networking.utils import package_message


def send_message(message, sender, target, security_config):
    """
    Send a message to the target address server.

    :param message:  The message to send
    :type message:  BaseMessage
    :param sender:  The message sender
    :type sender:  ActorAddress
    :param target:  The target for the message
    :type target:  ActorAddress
    :param security_config:  The security config
    :type security_config:  SocketServerSecurity
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if security_config.certfile:
            ssl_version = security_config.ssl_version
            ciphers = security_config.cipher
            ssl.wrap_socket(sock, ssl_version=ssl_version, ciphers=ciphers)
        sock.connect((target.host, target.port))
        message = package_message(
            message, sender, security_config, target)
        sock.send(message)
    finally:
        sock.close()
