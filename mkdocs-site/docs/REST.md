Dynomite provides some functionalities through REST calls in the admin port `$ curl http://localhost:22222/<arg>`. The following arguments have been implemented:

* `info`
* `help`
* `ping`: performs a Redis inline ping to test Dynomite and storage
* `describe`
* `loglevelup`: increase the log level. More info in [Debugging](https://github.com/Netflix/dynomite/wiki/Debugging)
* `logleveldown`: decrease the log level. More info in [Debugging](https://github.com/Netflix/dynomite/wiki/Debugging)
* `historeset`
* `cluster_describe`
* `get_consistency`: get the consistency level. More info in [Consistency](https://github.com/Netflix/dynomite/wiki/Consistency)
* `set_consistency`: set the consistency level. More info in [Consistency](https://github.com/Netflix/dynomite/wiki/Consistency)
* `get_timeout_factor`
* `set_timeout_factor`

# State

One can change the operation status of Dynomite dynamically by performing `$ curl http://localhost:22222/state/<arg>` and one of the following arguments

* `standby`: do not perform writes or reads
* `writes_only`: perform writes only
* `normal`: switch back to normal mode
* `resuming`