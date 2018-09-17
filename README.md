
![dynomite logo](images/dynomite-logo.png)


# Dynomite 

[![Build Status](https://secure.travis-ci.org/Netflix/dynomite.png)](http://travis-ci.org/Netflix/dynomite)
[![Dev chat at https://gitter.im/Netflix/dynomite](https://badges.gitter.im/Netflix/dynomite.svg)](https://gitter.im/Netflix/dynomite?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Apache V2 License](http://img.shields.io/badge/license-Apache%20V2-blue.svg)](https://github.com/Netflix/dynomite/blob/dev/LICENSE)

**Dynomite**, inspired by [Dynamo whitepaper](http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf), is a thin, distributed dynamo layer for different storage engines and protocols. Currently these include [Redis](http://redis.io) and [Memcached](http://www.memcached.org/).  Dynomite supports multi-datacenter replication and is designed for high availability.

The ultimate goal with Dynomite is to be able to implement high availability and cross-datacenter replication on storage engines that do not inherently provide that functionality. The implementation is efficient, not complex (few moving parts), and highly performant.

## Workflow

Every branch numbered like v0.5.9, v0.5.8 etc is stable and safe to use in production unless marked as pre-release. The [dev]( https://github.com/Netflix/dynomite/tree/dev ) branch is the development unstable branch. Over time master branch has fallen behind and is not maintained. We will eventually delete it and may or may not recreate it.

For questions or contributions, please consider reading [CONTRIBUTING.md](CONTRIBUTING.md).

## Build

To build Dynomite from source with _debug logs enabled_ and _assertions disabled_:

    $ git clone git@github.com:Netflix/dynomite.git
    $ cd dynomite
    $ autoreconf -fvi
    $ ./configure --enable-debug=yes
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
                      [-c conf file] [-p pid file] 

    Options:
      -h, --help              : this help
      -V, --version           : show version and exit
      -t, --test-conf         : test configuration for syntax errors and exit
      -g, --gossip            : enable gossip (default: disabled)
      -d, --daemonize         : run as a daemon
      -D, --describe-stats    : print stats description and exit
      -v, --verbosity=N       : set logging level (default: 5, min: 0, max: 11)
      -o, --output=S          : set logging file (default: stderr)
      -c, --conf-file=S       : set configuration file (default: conf/dynomite.yml)
      -p, --pid-file=S        : set pid file (default: off)
      -x, --admin-operation=N : set size of admin operation (default: 0)


## Configuration

Dynomite can be configured through a YAML file specified by the -c or --conf-file command-line argument on process start. The configuration files parses and understands the following keys:

+ **env**: Specify environment of a node.  Currently supports aws and network (for physical datacenter).
+ **datacenter**: The name of the datacenter.  Please refer to [architecture document](https://github.com/Netflix/dynomite/wiki/Architecture).
+ **rack**: The name of the rack.  Please refer to [architecture document](https://github.com/Netflix/dynomite/wiki/Architecture).
+ **dyn_listen**: The port that dynomite nodes use to inter-communicate and gossip.
+ **enable_gossip**: enable gossip instead of static tokens (default: false). Gossip is experimental.
+ **gos_interval**: The sleeping time in milliseconds at the end of a gossip round.
+ **tokens**: The token(s) owned by a node.  Currently, we don't support vnode yet so this only works with one token for the time being.
+ **dyn_seed_provider**: A seed provider implementation to provide a list of seed nodes.
+ **dyn_seeds**: A list of seed nodes in the format: address:port:rack:dc:tokens (note that vnode is not supported yet)
+ **listen**: The listening address and port (name:port or ip:port) for this server pool.
+ **timeout**: The timeout value in msec that we wait for to establish a connection to the server or receive a response from a server. By default, we wait indefinitely.
+ **preconnect**: A boolean value that controls if dynomite should preconnect to all the servers in this pool on process start. Defaults to false.
+ **data_store**: An integer value that controls if a server pool speaks redis (0) or memcached (1) or other protocol. Defaults to redis (0).
+ **auto_eject_hosts**: A boolean value that controls if server should be ejected temporarily when it fails consecutively server_failure_limit times. See [liveness recommendations](notes/recommendation.md#liveness) for information. Defaults to false.
+ **server_retry_timeout**: The timeout value in msec to wait for before retrying on a temporarily ejected server, when auto_eject_host is set to true. Defaults to 30000 msec.
+ **server_failure_limit**: The number of consecutive failures on a server that would lead to it being temporarily ejected when auto_eject_host is set to true. Defaults to 2.
+ **servers**: A list of local server address, port and weight (name:port:weight or ip:port:weight) for this server pool. Currently, there is just one.
+ **secure_server_option**: Encrypted communication. Must be one of 'none', 'rack', 'datacenter', or 'all'. ```datacenter``` means all communication between datacenters is encrypted but within a datacenter it is not. ```rack``` means all communication between racks and regions is encrypted however communication between nodes within the same rack is not encrypted. ```all``` means all communication between all nodes is encrypted. And ```none``` means none of the communication is encrypted. 
+ **stats_listen**: The address and port number for the REST endpoint and for accessing statistics.
+ **stats_interval**: set stats aggregation interval in msec (default: 30000 msec).
+ **mbuf_size**: size of mbuf chunk in bytes (default: 16384 bytes).
+ **max_msgs**: max number of messages to allocate (default: 200000).

For example, the configuration file in [conf/dynomite.yml](conf/dynomite.yml)

Finally, to make writing syntactically correct configuration files easier, dynomite provides a command-line argument -t or --test-conf that can be used to test the YAML configuration file for any syntax error.


## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
