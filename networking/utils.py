"""
Utilities for networking such as a socket for sending
a message.

@author aevans
"""
import json
import pickle
import socket


def package_message(message):
    """
    Package a string message

    :param message:  The message to package
    :type message:  str
    :return:  a bytes like message with appropriate length
    :rtype:  bytes
    """
    if type(message) is not str:
        raise ValueError('String Message Required in package_message')
    return "{}:::{}".format(str(len(message)),message).encode()


def package_dict_as_json_string(message):
    """
    Package a dictionary as json string in a byte array.

    :param message:  The message to package
    :type message:  dict
    :return:  Json string in a byte array
    :rtype:  bytes
    """
    if type(message) is not dict:
        raise ValueError('Dict required in package_dict_as_json')
    msg = json.dumps(message)
    return "{}:::{}".format(str(len(msg)), msg).encode()


def send_to_actor(message_string, target):
    """
    Perform the send operation to an actor

    :param message_string:  The packaged actor message
    :type message_string:  str
    :param target:  The target actor
    :type target:  ActorAddress
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((target.host, target.port))
        sock.send(message_string)
    finally:
        sock.close()


def send_message_to_actor(message_string, target, sender):
    """
    Send the message to an actor

    :param message_string:  The message string
    :type message_string:  str
    :param target:  The target address
    :type target:  ActorAddress
    :param sender:  The message sender
    :type sender:  ActorAddress
    """
    msg = {
        'message': message_string,
        'target': target.__repr__(),
        'sender': sender.__repr__()}
    msg = package_dict_as_json_string(msg)
    send_to_actor(msg, target)


def send_json_to_actor(message_json, target, sender):
    """
    Message json

    :param message_json:  The dictionary to send
    :type message_json:  dict
    :param target:  The target address
    :type target:  ActorAddress
    :param sender:  The message sender
    :type sender:  ActorAddress
    """
    message_json['target'] = target.__repr__()
    message_json['sender'] = sender.__repr__()
    msg = package_dict_as_json_string(message_json)
    send_to_actor(msg, target)
