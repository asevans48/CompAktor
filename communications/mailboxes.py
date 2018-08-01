"""
Stores the current actor mailboxes


@author aevans
"""
from actors.address.addressing import ActorAddress

mailboxes = {}


class MailboxNotFoundError(Exception):

    def __init__(self, message="Mailbox Not Found"):
        """
        Constructor

        :param message:  An error message
        :type message:  str
        """
        self.message = message


def append_mailbox(address, mailbox):
    """
    Append a mailbox to the dictionary

    :param address:  The address of the actor
    :type address:  ActorAddress
    :param mailbox:  The mailbox queue
    :type mailbox:  gevent.queue.Queue
    """
    if type(address) is ActorAddress:
        address = address.__repr__()
    global mailboxes
    mailboxes[address] = mailbox


def get_mailbox(address):
    """
    Get a mailbox by address
    :param address:  The address of the actor
    :type address:  str
    :return:  The actor mailbox
    :rtype:  gevent.queue.Queue
    """
    if type(address) is ActorAddress:
        address = ActorAddress.__repr__()

    if mailboxes.get(address, None):
        return mailboxes[address]
    else:
        raise MailboxNotFoundError


def remove_mailbox(address):
    """
    Remove a mailbox.
    :param address:  The actor address
    :type address:  str
    :return: The address
    :rtype:  str
    """
    if type(address) is ActorAddress:
        address = address.__repr__()
    global mailboxes
    if mailboxes.get(address, None):
        addr = mailboxes.pop(address)
        return addr
    else:
        return None
