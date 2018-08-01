"""
Actor registry

@author aevans
"""

from enum import Enum


class ActorStatus(Enum):
    SETUP = 1
    RUNNING = 2
    STOPPED = 3
    UNREACHABLE = 4


class ActorRegistry(object):
    """
    A basic actor registry. The registry maintains basic actor information.

    Actors can have 1 parent
    Actors can have multiple children
    Actors have 1 address
    """

    def __init__(self):
        """
        Constructur
        """
        self.registry = {}

    def add_actor(self, actor, actor_address, actor_status):
        """
        Adds the actor to the registry

        :param actor:  The actor registry
        :type actor:  BaseActor
        :param actor_address:   The actor address
        :type actor:  ActorAddress
        :param actor_status:  The status of the actor
        :type actor_status:  ActorStatus
        """
        if self.registry.get(actor_address.address, None) is None:
            self.registry[actor_address.address] = {
                'address': actor_address,
                'children': [],
                'actor': actor,
                'parent': None,
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


    def remove_actor(self, actor_address):
        """
        Remove an actor by address

        :param actor_address:  The actor address to remove
        :type actor_address:  ActorAddress
        """
        if self.registry.get(actor_address.address, None):
            self.registry.pop(actor_address.address)
