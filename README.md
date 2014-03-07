# Dynomite

**Dynomite** , inspired by Dynamo whitepaper, is a thin, distributed replication layer to different protocol storages. 


## Build

To build Dynomite from distribution tarbal:

    $ ./configure
    $ make
    $ sudo make install

To build Dynomite in _debug mode_:

    $ CFLAGS="-ggdb3 -O0" ./configure --enable-debug=full
    $ make
    $ sudo make install

To build Dynomite from source with _debug logs enabled_ and _assertions disabled_:

    $ git clone git@github.com:Netflix/dynomite.git
    $ cd dynomite
    $ autoreconf -fvi
    $ ./configure --enable-debug=log
    $ make
    $ src/dynomite -h


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

dynomite can be configured through a YAML file specified by the -c or --conf-file command-line argument on process start. The configuration file is used to specify the server pools and the servers within each pool that dynomite manages. The configuration files parses and understands the following keys:

+ **listen**: The listening address and port (name:port or ip:port) for this server pool.
+ **hash**: The name of the hash function. Possible values are:
 + one_at_a_time
 + md5
 + crc16
 + crc32 (crc32 implementation compatible with [libmemcached](http://libmemcached.org/))
 + crc32a (correct crc32 implementation as per the spec)
 + fnv1_64
 + fnv1a_64
 + fnv1_32
 + fnv1a_32
 + hsieh
 + murmur
 + jenkins
+ **hash_tag**: A two character string that specifies the part of the key used for hashing. Eg "{}" or "$$". [Hash tag](notes/recommendation.md#hash-tags)  enable mapping different keys to the same server as long as the part of the key within the tag is the same.
+ **distribution**: The key distribution mode. Possible values are:
 + ketama
 + modula
 + random
+ **timeout**: The timeout value in msec that we wait for to establish a connection to the server or receive a response from a server. By default, we wait indefinitely.
+ **backlog**: The TCP backlog argument. Defaults to 512.
+ **preconnect**: A boolean value that controls if dynomite should preconnect to all the servers in this pool on process start. Defaults to false.
+ **redis**: A boolean value that controls if a server pool speaks redis or memcached protocol. Defaults to false.
+ **server_connections**: The maximum number of connections that can be opened to each server. By default, we open at most 1 server connection.
+ **auto_eject_hosts**: A boolean value that controls if server should be ejected temporarily when it fails consecutively server_failure_limit times. See [liveness recommendations](notes/recommendation.md#liveness) for information. Defaults to false.
+ **server_retry_timeout**: The timeout value in msec to wait for before retrying on a temporarily ejected server, when auto_eject_host is set to true. Defaults to 30000 msec.
+ **server_failure_limit**: The number of conseutive failures on a server that would leads to it being temporarily ejected when auto_eject_host is set to true. Defaults to 2.
+ **servers**: A list of server address, port and weight (name:port:weight or ip:port:weight) for this server pool.


For example, the configuration file in [conf/dynomite.yml](conf/dynomite2.yml)
Finally, to make writing syntactically correct configuration file easier, dynomite provides a command-line argument -t or --test-conf that can be used to test the YAML configuration file for any syntax error.

## Observability

Observability in dynomite is through logs and stats.

Dynomite exposes stats at the granularity of server pool and servers per pool through the stats monitoring port. The stats are essentially JSON formatted key-value pairs, with the keys corresponding to counter names. By default stats are exposed on port 22222 and aggregated every 30 seconds. Both these values can be configured on program start using the -c or --conf-file and -i or --stats-interval command-line arguments respectively. You can print the description of all stats exported by dynomite using the -D or --describe-stats command-line argument.

    $ dynomite --describe-stats

    pool stats:
      client_eof          "# eof on client connections"
      client_err          "# errors on client connections"
      client_connections  "# active client connections"
      server_ejects       "# times backend server was ejected"
      forward_error       "# times we encountered a forwarding error"
      fragments           "# fragments created from a multi-vector request"

    server stats:
      server_eof          "# eof on server connections"
      server_err          "# errors on server connections"
      server_timedout     "# timeouts on server connections"
      server_connections  "# active server connections"
      requests            "# requests"
      request_bytes       "total request bytes"
      responses           "# respones"
      response_bytes      "total response bytes"
      in_queue            "# requests in incoming queue"
      in_queue_bytes      "current request bytes in incoming queue"
      out_queue           "# requests in outgoing queue"
      out_queue_bytes     "current request bytes in outgoing queue"

Logging in dynomite is only available when dynomite is built with logging enabled. By default logs are written to stderr. Dynomite can also be configured to write logs to a specific file through the -o or --output command-line argument. On a running dynomite, we can turn log levels up and down by sending it SIGTTIN and SIGTTOU signals respectively and reopen log files by sending it SIGHUP signal.



## License

Copyright 2014 Netflix, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
