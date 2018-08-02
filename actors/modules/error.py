

def prep_not_handled_in_receive_message(message, actor_address):
    """
    Prepare a message for when a message is not handled in the receipt.

    :param message:  The message that is not handled
    :type message:  BaseMessage
    :param actor_address:  The actor address where this error occurs
    :type actor_address:  ActorAddress
    :return:  The prepared message
    :rtype:  str
    """
    addr_part = "{} @ {}:{}".format(
        actor_address.address,
        actor_address.host,
        actor_address.port)
    out_message = "{} ::: Not Handled in Receive {}".format(
        type(message),
        addr_part)


def raise_not_handled_in_receipt(message, actor_address):
    """
    Rase an error when a message is unhandled.

    :param message:  The message that is not handled
    :type message:  BaseMessage
    :param actor_address:  The calling actor address
    :type actor_address:  ActorAddress
    """
    message = prep_not_handled_in_receive_message(message, actor_address)
    raise NotImplemented(message)
