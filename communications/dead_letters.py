"""
The dead letter handler.  Can be overwritten as needed


@auhtor aevans
"""


def handle_dead_letters(message, sender):
    """
    The dead letter handler.  Override this method.
    :param message:  The message to handle
    :type message:  str
    :param sender:  The message sender
    :type sender:  str
    """
    pass
