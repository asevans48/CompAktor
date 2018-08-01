
class ActorCleanup(object):
    """
    Message for cleaning up the actor
    """

    def __init__(self, actor_address):
        """
        Constructor

        :param actor_address:  The actor address
        :type actor_address:  ActorAddress
        """
        self.actor_address = actor_address


class RegisterActor(object):
    """
    Message for registering an actor.
    """

    def __init__(self, actor_address, actor_status):
        """
        Register the actor

        :param actor_address:  The actor address
        :type actor_address:  ActorAddress
        :param actor_status:  The current status of the actor
        :type actor_status:  The actor status
        """
        self.actor_address = actor_address
        self.actor_status = actor_status


class AddChild(object):
    """
    The actor child
    """

    def __init__(self, parent_address, child_address):
        """
        Add a child to the parent

        :param parent_address:  The parent address
        :type parent_address:  ActorAddress
        :param child_address:  The child address
        :type child_address:  ActorAddress
        """
        self.parent_address = parent_address
        self.child_address = child_address


class RemoveChild(object):
    """
    Remove the child from the parent
    """

    def __init__(self, parent_address, child_address):
        """
        Constructor

        :param parent_address:  The parent address
        :type parent_address:  ActorAddress
        :param child_address:  The child address
        :type child_address:  ActorAddress
        """
        self.parent_address = parent_address
        self.child_address = child_address


class SetActorStatus(object):
    """
    Set an actor status
    """

    def __init__(self, actor_address, status):
        """
        Set the actor status.

        :param actor_address:  The actor address.
        :type actor_address:  ActorAddress
        :param status:  The status to set
        :type status:  int
        """
        self.actor_address = actor_address
        self.status = status
