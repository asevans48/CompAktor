"""
The actor registry.

@author aevans
"""
import atexit
from time import sleep

from gevent.queue import Queue

from actors.base_actor import BaseActor, WorkPoolType, ActorConfig
from actors.modules.error import raise_not_handled_in_receipt
from messages.actor_maintenance import RemoveActor, RegisterActor, CreateActor, RemoveChild, StopActor, ActorInf, \
    RegisterGlobalActor, UnRegisterGlobalActor
from messages.base import BaseMessage
from messages.routing import ReturnMessage, Tell, Ask, Broadcast
from messages.system_maintenance import SetConventionLeader
from networking.socket_server import SocketServerSecurity, create_socket_server
from networking.utils import package_message
from registry.registry import ActorRegistry
from system.actors.message_actor import MessageActor


class ActorSystem(BaseActor):

    def __init__(self, actor_config):
        """
        Constructor

        :param actor_config:  The system is a separate process running on an actor
        :type actor_config:  ActorConfig
        :param security_config:  The security configuration
        :type security_config:  SocketServerSecurity
        """
        actor_config.work_pool_type = WorkPoolType.NO_POOL
        super(ActorSystem, self).__init__(actor_config, self.message_queue)
        self.is_convention_leader = False
        self.server = None
        self.remote_systems = {}
        self.global_actors = {}
        if self.config.host and self.config.port > 2000:
            self.server = create_socket_server(
                self.config.host,
                self.config.port,
                security_config=self.config.security_config)
            self.server.signal_queue.get(timeout=10)
        self.registery = ActorRegistry()
        self.message_queue = Queue()
        if self.config.convention_leader is None:
            self.is_convention_leader = True
            self.config.convention_leader = self.config.myAddress

    def __set_convention_leader(self, message, sender):
        """
        Sets the convention leader

        :param message:  The message to handler
        :type message:  SetConventionLeader
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        convention_address = message.actor_address
        my_addr = self.config.myAddress
        if convention_address.address is my_addr.address\
                and convention_address.host is my_addr.host\
                and convention_address.port is my_addr.port:
            self.is_convention_leader = True
        self.config.convention_leader = convention_address
        my_addr = self.config.myAddress
        message.target = message.sender
        message.sender = my_addr
        return message

    def __remove_actor(self, message, sender):
        """
        Handle the removal of an actor from the system.

        :param message:  The message to handle
        :type message:
        :param sender:
        """
        inf = None
        addr = message.actor_address
        if self.registery.get(addr.address, None):
            msg = StopActor(addr, self.config.myAddress)
            self.send(addr, msg)
            inf = self.registery.remove_actor(addr, terminate=False)
        my_addr = self.config.myAddress
        return ActorInf(inf, target=sender, sender=my_addr)

    def __unregister_global_actor(self, message, sender):
        """
        Unregister the actor.

        :param message:  The message to handle
        :type message:  RegisterActor
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        global_name = message.global_name
        actor_address = message.actor_address
        if self.global_actors.get(global_name, None):
            self.global_actors.pop(global_name)

    def __register_global_actor(self, message, sender):
        """
        Register the actor.

        :param message:  The message to handle
        :type message:  RegisterActor
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        global_name = message.global_name
        actor_address = message.actor_address
        self.global_actors[global_name] = actor_address


    def __remove_child(self, message, sender):
        """
        Remove a child from an actor.  Assumes actor is stopped

        :param message:  The message to handle
        :type message:  RemoveChild
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        parent = message.parent_address
        child = message.child_address
        self.registery.remove_child(parent, child)

    def __handle_ask(self, message, sender):
        """
        Remove a child from an actor.  Assumes actor is stopped

        :param message:  The message to handle
        :type message:  RemoveChild
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        msg = message.message
        rval = self.receive(msg, sender)
        my_addr = self.config.myAddress
        if rval and issubclass(rval, BaseMessage):
            omessage = rval
        else:
            omessage = ReturnMessage(rval, sender, my_addr)
        self.send(omessage, my_addr)

    def receive(self, message, sender):
        """
        Handle the receipt of a message.

        :param message:  The received message
        :type message:  object
        :param sender:  The message sender
        :type sender:  ActorAddress
        """
        if type(message) is SetConventionLeader:
            self.__set_convention_leader(message, sender)
        elif type(message) is RemoveActor:
            self.__remove_actor(message, sender)
        elif type(message) is RegisterGlobalActor:
            self.__register_global_actor(message, sender)
        elif type(message) is UnRegisterGlobalActor:
            self.__unregister_global_actor(message, sender)
        elif type(message) is CreateActor():
            self.__create_actor(message, sender)
        elif type(message) is RemoveChild:
            self.__remove_child(message, sender)
        else:
            my_addr = self.config.myAddress
            raise_not_handled_in_receipt(message, my_addr)


class ActorSystemHandler(object):

    def __init__(self, actor_config):
        """
        The users entry point to the system.

        :param actor_config:  The configuration for the actor system
        :type actor_config:  ActorConfig
        """
        self.__system_config = actor_config
        self.__system_actor = None
        self.__system_process = None
        self.__message_actor = None
        self.__message_process = None
        atexit.register(self.shutdown)

    def create_actor(self, actor_class, actor_config, system_address):
        """
        Create an actor on a system with the system as parent

        :param system_addres:  The actor system
        :type system_address:  ActorAddress
        :param actor_config:  The actor config
        :type actor_config:  ActorConfig
        """
        if self.__message_actor is None:
            self.__create_message_actor()
        actor = self.__message_actor
        actor.config.myAddress.parent = [self.__system_config.myAddress.address, ]
        message = CreateActor(
            actor_class,
            actor_config,
            system_address,
            system_address,
            actor.config.myAddress.parent)
        scfg = actor.config.security_config
        m_addr = actor.config.myAddress
        message = package_message(message, m_addr, scfg, system_address)
        actor.config.mailbox.put_nowait(message)

    def __create_message_actor(self):
        if self.__system_process and self.__system_process.is_alive:
            self.__message_actor = MessageActor(
                self.__system_config,
                self.__system_actor.config.myAddress)
            self.__message_process = self.__message_actor.start()
            sys_addr = self.__system_actor.config.myAddress
            self.create_actor(MessageActor, self.__system_config, sys_addr)

    def tell(self, system, message, target, sender=None):
        """
        Tell the system

        :param system:  The actor system
        :type system:  ActorAddress
        :param message:  The message to handle
        :type message:  BaseMessage
        :param target:  The target actor address
        :type target:   ActorAddress
        :param sender:  The sender address defaulting to target
        :type sender:  ActorAddress
        """
        if self.__message_actor is None:
            self.__create_message_actor()
        if sender is None:
            sender = self.__message_actor.config.myAddress
        sec = self.__system_config.security_config
        message = Tell(message, target, sender)
        message = package_message(message, sender, sec, target)
        self.__message_actor.config.queue.put_nowait(message)

    def ask(self, system, message, target, sender=None, timeout=30):
        """
        Ask for a response from the sytem. First response wins.

        :param system:  The system address
        :type system:  ActorAddress
        :param message:  The message to send
        :type message:  BaseMessage
        :param target:  The target actor address
        :type target:   ActorAddress
        :param sender:  The sender address defaulting to target
        :type sender:  ActorAddress
        :param timeout:  Time to wait for system response
        :type timeout:  int
        :return:  A new ask request with a wrapped response
        :rtype:  BaseMessage
        """
        if self.__message_actor is None:
            self.__create_message_actor()
        if sender is None:
            sender = self.__message_actor.config.myAddress
        sec = self.__system_config.security_config
        message = Ask(message, target, sender)
        message = package_message(message, sender, sec, target)
        self.__message_actor.config.queue.put_nowait(message)
        sleep(1)
        rval = self.__message_actor.config.queue.get(timeout=timeout)
        if type(rval) is Ask:
            return rval.message
        else:
            raise ValueError('Failed to Receive Response')

    def broadcast(self, system, message, target, sender=None):
        """
        Tell the system

        :param system:  The actor system
        :type system:  ActorAddress
        :param message:  The message to handle
        :type message:  BaseMessage
        :param target:  The target actor address
        :type target:   ActorAddress
        :param sender:  The sender address defaulting to target
        :type sender:  ActorAddress
        """
        if self.__message_actor is None:
            self.__create_message_actor()
        if sender is None:
            sender = self.__message_actor.config.myAddress
        sec = self.__system_config.security_config
        message = Broadcast(message, target, sender)
        message = package_message(message, sender, sec, target)
        self.__message_actor.config.queue.put_nowait(message)

    def start_system(self):
        self.__system_actor = ActorSystem(self.__system_config)
        self.__system_process = self.__system_actor.start()

    def shutdown(self):
        sys_addr = self.__system_actor.config.myAddress
        p = self.__system_process
        if p.is_alive:
            message = StopActor()
            sec_cfg = self.__system_actor.config.security
            message = package_message(message, sys_addr, sec_cfg, sys_addr)
            self.__system_actor.config.mailbox.put_nowait(message)
            p.terminate()
            p.join(timeout=120)
