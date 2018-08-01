"""
Actor registry

@author aevans
"""
import traceback
from enum import Enum

from logging_handler import logging
from logging_handler.logging import package_actor_message, package_error_message


class ActorStatus(Enum):
    SETUP = 1
    RUNNING = 2
    STOPPED = 3
    UNREACHABLE = 4


class ActorRegistry(object):
    """
    A basic actor registry. The registry maintains basic actor information.
    Solving the reader writer problem is crucial here.

    Actors can have 1 parent
    Actors can have multiple children
    Actors have 1 address
    """

    def __init__(self):
        """
        Constructur
        """
        self.registry = {}

    def add_actor(self, actor_address, actor_status, actor_proc=None):
        """
        Adds the actor to the registry

        :param actor_proc:  The actor process if applicable
        :type actor_proc:  Process
        :param actor_address:   The actor address
        :type actor:  ActorAddress
        :param actor_status:  The status of the actor
        :type actor_status:  ActorStatus
        """
        if self.registry.get(actor_address.address, None) is None:
            self.registry[actor_address.address] = {
                'address': actor_address,
                'children': [],
                'parent': None,
                'actor_proc': actor_proc,
                'status': actor_status
            }
        else:
            raise ValueError('Registry Already Contains Actor')

    def add_child(self, parent_address, child_address):
        """
        Adds a child to the registry at the parent.

        :param parent_address:  The parent address
        :type parent_address:  ActorAddress
        :param child_address:  The child address
        :type child_address:  ActorAddress
        """
        if self.registry.get(parent_address.address, None) is not None:
            if self.registry.get(child_address.address, None) is not None:
                if self.registry[child_address.address]['parent'] is None:
                    actor = self.registry[parent_address.address]
                    if child_address.address not in actor.children:
                        actor.children.append(child_address.address)

    def remove_child(self, parent_address, child_address):
        """
        Remove the child at the parent if it exists
        :param parent_address:  The parent actor address
        :type parent_address:  ActorAddress
        :param child_address:  The child actor address
        :type child_address:  ActorAddress
        """
        if self.registry.get(child_address.address, None):
            if self.registry.get(parent_address.address, None):
                actor = self.registry[parent_address.address]
                if child_address.address in actor.children:
                    actor.children.remove(child_address.address)


    def set_actor_status(self, actor_address, status):
        """
        Set the current actor status.

        :param actor_address:  The actor address
        :type actor_address:  ActorAddress
        :param status:   The new status
        :type status:  ActorStatus
        """
        if self.registry.get(actor_address.address, None):
            self.registry[actor_address.address]['status'] = status


    def remove_actor(self, actor_address, terminate=True):
        """
        Remove an actor by address

        :param actor_address:  The actor address to remove
        :type actor_address:  ActorAddress
        :param terminate:  Terminates an actor as possible
        :type terminate:  boolean
        :return:  Actor information
        :rtype:  dict
        """
        actor_inf = None
        if self.registry.get(actor_address.address, None):
            actor_inf = self.registry.pop(actor_address.address)
            if terminate:
                proc = actor_inf['actor_proc']
                if proc:
                    try:
                        proc.terminate()
                        proc.join()
                    except Exception as e:
                        logger = logging.get_logger()
                        message = package_error_message()
                        logging.log_error(logger, message)

    def get_actor(self, actor_address, default=None):
        """
        Get the required actor.

        :param actor_address:  The actor address
        :type actor_address:  ActorAddress
        :return:  The discovered actor information
        :param default:  Any default to return on failed discovery
        :type default:  object
        :rtype:  dict
        """
        return self.registry.get(actor_address.address, default)
