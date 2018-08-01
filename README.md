# CompAktor

A computational actor system based on gevent, multiprocessing and the actor model concept.  The intent of this application is to handle networking, updates, and autonomation.

The hope is to deploy this project in our proprietary backend tools.

The socket server runs in a greenlet StreamServer.

This project will attempt to settle on 1 of 2 tpyes of actors

  - actors run in processe and maintain greenlet pools (or other types of pools for certain tasks)
  - actors run in greenlets themselves (risky) and execute on different processes

This project is very much in development.

# License

Apache v 2.0