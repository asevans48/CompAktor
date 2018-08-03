
import base64
import pickle

from actors.address.addressing import ActorAddress


def get_object_from_message(message):
    """
    Obtained the message instance from a packaged message

    :param message:  The message dictionary
    :type message:  dict
    :return:  The message object
    :rtype:  object
    """
    clz = base64.b64decode(message['message'])
    clz = pickle.loads(clz)
    return clz


def get_message_sender(message):
    """
    Obtain the sender from a message.

    :param message:  The message to handle
    :type message:  dict
    :return:  The message sender or None if missing
    :rtype:  ActorAddress
    """
    if message.get('sender', None):
        sender_list = message['sender']
        return ActorAddress(sender_list[0], sender_list[1], sender_list[2])
    return None


def unpack_message(message):
    """
    Unpack the message
    :param message:  The message to unpack
    :type message:  BaseMessage
    :return:  A tuple containing the message and sender
    :rtype:  tuple
    """
    message = get_object_from_message(message)
    sender = get_message_sender(message)
    return (message, sender)
