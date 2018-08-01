import json

from actors.base_actor import BaseActor, ActorConfig, WorkPoolType


class ActorMessageRouter(BaseActor):

    def __init__(self, host, port):
        actor_config = ActorConfig()
        actor_config.work_pool_type = WorkPoolType.NO_POOL
        actor_config.port = port
        actor_config.host = host
        BaseActor.__init__(self, actor_config)

    def receive(self, message):
        message = json.loads(message)
        message_type = message.get('type', None)
