"""
Utilities for networking such as a socket for sending
a message.

@author aevans
"""
import base64
import hmac
import json
import pickle
import socket


def send_to_actor(message_string, target):
    """
    Send a packaged message to an actor.

    :param message_string:  The message string
    :type message_string:  str
    :param target:  The target actor
    :type sender:  ActorAddress
    """
    if target is not None and message_string is not None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(2)
            sock.connect((target.host, target.port))
            sock.send(message_string)
        finally:
            sock.close()


def package_message(message, sender, security_config, target=None):
    """
    Package a string message

    :param message:  The message to package
    :type message:  object
    :param security_config:  The security config from Socket Server
    :type socket_server: socket_server.SocketServerSecurity
    :return:  a bytes like message with appropriate length
    :rtype:  bytes
    """
    omessage = base64.b64encode(pickle.dumps(message))
    omessage = {'message': omessage.decode(),
                'sender': (sender.address, sender.host, sender.port),
                'sender_addr': sender.__repr__()}
    if target:
        omessage['target'] = target.__repr__()
    omessage = json.dumps(omessage)
    obytes = omessage.encode()
    signature = hmac.new(security_config.get_key(), obytes, security_config.hashfunction)
    signature = signature.digest()
    assert len(signature) == security_config.get_hash_size()
    return "{}:::{}:::{}:::{}".format(
        str(security_config.get_magic()),
        base64.b64encode(signature).decode(),
        len(omessage),
        omessage).encode()


def send_message_to_actor(message, target, sender, security_config):
    """
    Send the message to an actor

    :param message:  The message to package
    :type message_string:  object
    :param target:  The target address
    :type target:  ActorAddress
    :param sender:  The message sender
    :type sender:  ActorAddress
    """
    msg = package_message(
        message,
        sender,
        security_config,
        target=None)
    send_to_actor(msg, target)
