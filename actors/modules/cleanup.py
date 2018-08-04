"""
Cleanup utilities shared by all actors

@author aevans
"""
import traceback

from logging_handler import logging
from logging_handler.logging import package_actor_message, package_error_message
from messages.actor_maintenance import ActorCleanup


def cleanup_actor(actor_address, system_queue, timeout=15):
    """
    Cleanup the actor by removing it from registry

    :param actor_address:  The address of the actor to cleanup
    :type actor_address:  ActorAddress
    :param system_queue:  The actor system queue
    :type system_queue:  multiprocessing.Queue
    :param timeout:  The timeout for the actor
    :type timeout:  int
    """
    try:
        message = ActorCleanup(actor_address)
        system_queue.put(message, timeout=timeout)
    except Exception as e:
        message = package_error_message(actor_address)
        logger = logging.get_logger()
        logging.log_error(logger, message)
