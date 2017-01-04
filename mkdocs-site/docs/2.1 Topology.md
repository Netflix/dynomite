A Dynomite cluster consists of multiple data centers (dc). A datacenter  is a group of racks and rack is a group of nodes. Each rack consists of the entire dataset, which is partitioned across multiple nodes in that rack. Hence multiple racks enable higher availability for data. Each node in a rack has a unique token, which helps to identify which dataset it owns. 

<img src="/Netflix/dynomite/wiki/images/topology1.png" width="500px" />

Each Dynomite node (e.g., a1 or b1 or c1)  has a Dynomite process co-located with the datastore server, which acts as a proxy, traffic router, coordinator and gossiper. In the context of the Dynamo paper, Dynomite is the Dynamo layer with additional support for pluggable datastore proxy, with an effort to preserve the native datastore protocol as much as possible.  

A datastore can be either a volatile datastore such as Memcached or Redis, or persistent datastore such as Mysql, BerkeleyDb or LevelDb.  Our current open sourced Dynomite offering supports Redis and Memcached.

<img src="/Netflix/dynomite/wiki/images/topology2.png" width="200px" />
