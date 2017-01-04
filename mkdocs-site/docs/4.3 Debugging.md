You can use the admin port to increase the log level in order to identify the root cause of an issue. The following curl call will increase the log level. The default is 5. 

    $ curl http://localhost:22222/loglevelup

Note that the higher the log level, there is a performance hit from writing logs. This is though only valid if Dynomite receives multiple thousands of OPS. To decrease the log level.

    $ curl http://localhost:22222/logleveldown




