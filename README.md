<!--
# Dynomite
-->
![dynomite logo](images/dynomite-logo.png?raw=true =150x150) </br>
**Dynomite**, inspired by Dynamo whitepaper, is a thin, distributed dynamo layer for different storages and protocols. 

When Dynomite team decided to settle on using C language, we found Twemproxy and did a fork in order to provide more features than just being a proxy layer.


## Build

To build Dynomite from source with _debug logs enabled_ and _assertions disabled_:

    $ git clone git@github.com:Netflix/dynomite.git
    $ cd dynomite
    $ autoreconf -fvi
    $ ./configure --enable-debug=log
    $ make
    $ src/dynomite -h

To build Dynomite in _debug mode_:

    $ git clone git@github.com:Netflix/dynomite.git
    $ cd dynomite
    $ autoreconf -fvi
    $ CFLAGS="-ggdb3 -O0" ./configure --enable-debug=full
    $ make
    $ sudo make install
    
## Help

    Usage: dynomite [-?hVdDt] [-v verbosity level] [-o output file]
                      [-c conf file] [-s stats port] [-a stats addr]
                      [-i stats interval] [-p pid file] [-m mbuf size]

    Options:
      -h, --help             : this help
      -V, --version          : show version and exit
      -t, --test-conf        : test configuration for syntax errors and exit
      -d, --daemonize        : run as a daemon
      -D, --describe-stats   : print stats description and exit
      -v, --verbosity=N      : set logging level (default: 5, min: 0, max: 11)
      -o, --output=S         : set logging file (default: stderr)
      -c, --conf-file=S      : set configuration file (default: conf/dynomite.yml)
      -s, --stats-port=N     : set stats monitoring port (default: 22222)
      -a, --stats-addr=S     : set stats monitoring ip (default: 0.0.0.0)
      -i, --stats-interval=N : set stats aggregation interval in msec (default: 30000 msec)
      -p, --pid-file=S       : set pid file (default: off)
      -m, --mbuf-size=N      : set size of mbuf chunk in bytes (default: 16384 bytes)


## Configuration

Dynomite can be configured through a YAML file specified by the -c or --conf-file command-line argument on process start. The configuration files parses and understands the following keys:

+ **env**: Specify environment of a node.  Currently supports aws and network (for physical datacenter).
+ **datacenter**: The name of the datacenter.  Please refer to [architecture document](https://github.com/Netflix/dynomite/wiki/Architecture).
+ **rack**: The name of the rack.  Please refer to [architecture document](https://github.com/Netflix/dynomite/wiki/Architecture).
+ **dyn_listen**: The port that dynomite nodes use to inter-communicate and gossip.
+ **gos_interval**: The sleeping time in milliseconds at the end of a gossip round.
+ **tokens**: The token(s) owned by a node.  Currently, we don't support vnode yet so this only works with one token for the time being.
+ **dyn_seed_provider**: A seed provider implementation to provide a list of seed nodes.
+ **dyn_seeds**: A list of seed nodes in the format: address:port:rack:dc:tokens (node that vnode is not supported yet)
+ **listen**: The listening address and port (name:port or ip:port) for this server pool.
+ **timeout**: The timeout value in msec that we wait for to establish a connection to the server or receive a response from a server. By default, we wait indefinitely.
+ **preconnect**: A boolean value that controls if dynomite should preconnect to all the servers in this pool on process start. Defaults to false.
+ **redis**: A boolean value that controls if a server pool speaks redis or memcached protocol. Defaults to false.
+ **server_connections**: The maximum number of connections that can be opened to each server. By default, we open at most 1 server connection.
+ **auto_eject_hosts**: A boolean value that controls if server should be ejected temporarily when it fails consecutively server_failure_limit times. See [liveness recommendations](notes/recommendation.md#liveness) for information. Defaults to false.
+ **server_retry_timeout**: The timeout value in msec to wait for before retrying on a temporarily ejected server, when auto_eject_host is set to true. Defaults to 30000 msec.
+ **server_failure_limit**: The number of consecutive failures on a server that would lead to it being temporarily ejected when auto_eject_host is set to true. Defaults to 2.
+ **servers**: A list of local server address, port and weight (name:port:weight or ip:port:weight) for this server pool. Usually there is just one.

For example, the configuration file in [conf/dynomite.yml](conf/dynomite.yml)

Finally, to make writing syntactically correct configuration files easier, dynomite provides a command-line argument -t or --test-conf that can be used to test the YAML configuration file for any syntax error.



## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
