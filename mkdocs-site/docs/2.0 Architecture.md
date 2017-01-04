## Architecture Overview 
In the open source world, there are various single-server datastore solutions, e.g. Memcached, Redis, BerkeleyDb, LevelDb, Mysql (datastore).  The availability story for these single-server datastores usually ends up being a master-slave setup. Once traffic demands overrun this setup, the next logical progression is to introduce sharding.  Most would agree that it is non trivial to operate this kind of a setup. Furthermore, managing data from different shards is also a challenge for application developers.

In the age of high scalability and big data, Dynomite’s design goal is to turn those single-server datastore solutions into peer-to-peer, linearly scalable, clustered systems while still preserving the native client/server protocols of the datastores, e.g., Redis protocol.

Dynomite and the target storage engine run on the same node. Clients connect to Dynomite, and requests are proxied to either the storage engine on the same node or to Dynomite processes running on other nodes.   

[[images/dynomite-architecture.png]]  

As the request goes through a Dynomite node, the data gets replicated and eventually stored in the target storage.  The data can then be read back either through Dynomite or directly from the underlying storage’s API. 