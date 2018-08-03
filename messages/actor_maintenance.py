"""
Messages for maintaining actors in the system and registry.

@author aevans
"""
from messages.base import BaseMessage


class ActorInf(BaseMessage):
    """
    Actor information
    """

    def __init__(self, actor_inf, target, sender):
        """
        Constructor

        :param actor_inf:  The actor information from the registry
        :type actor_inf:  dict
        :param target:  The target actor address
        :type  target:  ActorAddress
        :param sender:  The sender address
        :type sender:  ActorAddress
        """
        super(ActorInf, self).__init__(target, sender)
        self.actor_inf = actor_inf


class ActorCleanup(BaseMessage):
    """
    Message for cleaning up the actor
    """

    def __init__(self, actor_address, target, sender):
        """
        Constructor

        :param actor_address:  The actor address
        :type actor_address:  ActorAddress
        :param target:  The target address
        :type target:  ActorAddress
        :param sender:  The sender address
        :type sender:  ActorAddress
        """
        super(ActorCleanup, self).__init__(target, sender)
        self.actor_address = actor_address


class CreateActor(BaseMessage):
    """
    Message for the creation of an actor
    """

    def __init__(self, actor_class, actor_config, parent_address, target, sender):
        """
        Constructor

        :param actor_class:  The class for the actor to be created
        :type actor_class:  object
        :param actor_config:  The actor configuration
        :type actor_config:  ActorConfig
        :param parent_address:  The address of the parent
        :type parent_address:  ActorAddreses
        :param target:  The target address
        :type target:  ActorAddress
        :param sender:  The sender address
        :type sender:  ActorAddress
        """
        super(CreateActor, self).__init__(target, sender)
        self.actor_class = actor_class
        self.actor_config = actor_config
        self.parent_address = parent_address


class RegisterActor(BaseMessage):
    """
    Message for registering an actor.
    """

    def __init__(self, actor_address, actor_status, target, sender):
        """
        Constructor

        :param actor_address:  The actor address
        :type actor_address:  ActorAddress
        :param actor_status:  The current status of the actor
        :type actor_status:  The actor status
        :param target:  The target address
        :type target:  ActorAddress
        :param sender:  The sender address
        :type sender:  ActorAddress
        """
        super(RegisterActor, self).__init__(target, sender)
        self.actor_address = actor_address
        self.actor_status = actor_status


class UnRegisterGlobalActor(RegisterActor):
    """
    Unregister a global actor
    """

    def __init__(self, global_name, actor_address, target, sender):
        """
        Constructor

        :param global_name:  The global actor name
        :type global_name:  str
        :param actor_address:  The actor address
        :type actor_address:  ActorAddress
        :param target:  The target address
        :type target:  ActorAddress
        :param sender:  The sender address
        :type sender:  ActorAddress
        """
        super(RegisterGlobalActor, self).__init__(actor_address, None, target, sender)
        self.global_name = global_name


class RegisterGlobalActor(RegisterActor):
    """
    Register a global actor and the global actor address
    """

    def __init__(self, global_name, actor_address, target, sender):
        """
        Constructor

        :param global_name:  The global actor name
        :type global_name:  str
        :param actor_address:  The actor address
        :type actor_address:  ActorAddress
        :param target:  The target address
        :type target:  ActorAddress
        :param sender:  The sender address
        :type sender:  ActorAddress
        """
        super(RegisterGlobalActor, self).__init__(actor_address, None, target, sender)
        self.global_name = global_name


class RemoveActor(BaseMessage):
    """
    Removes an actor without effecting the actor status
    """

    def __init__(self, actor_address, target, sender):
        """
        Constructor

        :param actor_address:  The address to remove
        :type actor_address:  ActorAddress
        :param target:  The target address
        :type target:  ActorAddress
        :param sender:  The sender address
        :type sender:  ActorAddress
        """
        super(RemoveActor, self).__init__(target, sender)
        self.actor_address = actor_address

class SetActorStatus(BaseMessage):
    """
    Set an actor status
    """

    def __init__(self, actor_address, status, target, sender):
        """
        Constructor

        :param actor_address:  The actor address.
        :type actor_address:  ActorAddress
        :param status:  The status to set
        :type status:  int
        :param target:  The target address
        :type target:  ActorAddress
        :param sender:  The sender address
        :type sender:  ActorAddress
        """
        super(SetActorStatus, self).__init__(target, sender)
        self.actor_address = actor_address
        self.status = status


class StopActor(BaseMessage):
    """
    Stop an actor
    """

    def __init__(self, target, sender):
        """
        Constructor

        :param target:  The target for the message
        :type target:  ActorAddress
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        super(StopActor, self).__init__(target, sender)


class StopChildActor(BaseMessage):
    """
    Stop a child actor
    """

    def __init__(self, child_address, target, sender):
        """
        Constructor

        :param child_address:  The address of the child
        :type child_address:  ActorAddress
        :param target:  The target actor address
        :type target:  ActorAddress
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        super(StopChildActor, self).__init__(target, sender)
        self.child_address = child_address


class GetActorStatus(BaseMessage):
    """
    Get an actor status
    """

    def __init__(self, path, target, sender):
        """
        Constructor

        :param path:  The path to the actor
        :type path:  list
        :param target:  The target actor
        :type target:  ActorAddress
        :param sender:  The sender actor
        :type sender:  ActorAddress
        """
        super(GetActorStatus, self).__init__(target, sender)
        self.path = path
