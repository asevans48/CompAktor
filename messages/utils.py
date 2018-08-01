
import base64
import pickle


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
