###What is the relationship between a rack and a data center?
A Dynomite cluster consists of a group of data centers and each data center has a number of racks.
Each rack consists of a number of nodes.  Note that different racks can have different numbers of nodes.  Similarly, different data centers can have different numbers of racks.


###What is a replication factor? 
In a Dynomite cluster, each rack contains a complete set of data. Replication factor is the number of complete data copies in a data center.  In other words, it is the number of racks in a data center.

###What are the currently supported datastore servers?
Redis and Memcached, as well as other datastores through a protocol translation. We are currently experimenting with RocksDB, ForestDB and LMDB.

###Why not just use Cassandra?
Cassandra is written in Java and at runtime, GC trigger can result in unexpected latencies in some cases.  Cassandra is fast for writing but in very high frequency writes, Cassandra node will get blown up very soon as the compaction can't catch up. Also during a compaction is running on a Cassandra node, read latency is also affected due to the extra CPU and disk loads. 

Furthermore, to achieve the same throughputs, and latencies as Dynomite, using Cassandra, you probably need to spend 15X-20X of the cost of a Dynomite cluster. 

If you need more consistency, Cassandra at this point provides a better solution. However, Dynomite is going to provide different levels of consistencies very soon.  Stay tuned.

###Why not use McRouter? 
Do you employ a Read-Modify-Write pattern? Do you love Redis and its API (set, list, hash)?  Do you want to simplify your cluster management leveraging a peer-to-peer system? Do you want to avoid an extra network hop that McRouter introduces? If you answered yes to these questions, then Dynomite is for you.

###Why not use Redis cluster? 
Dynomite is a peer-to-peer system.  Redis cluster is multi master/slave system.  You should choose whichever system works best for you.

###Why not use Twemproxy?
Twemproxy, as the name recalls, is just a proxy to route Memcached or Redis requests in a consistent hashing mechanism to a pool of Memcached or Redis servers. In a way, it is similar to McRouter but without any replications. You can consider Dynomite as a superset of Twemproxy's features. Indeed, Dynomite was originally a fork of Twemproxy before we went off to our own path.   

###What do we need to install before building Dynomite?
You need to install libevent, autoconf, libtool, and either Memcached or Redis

###How can we monitor a Dynomite process?
There is an admin port that you can access with HTTP protocol to pull out statistical information from a running Dynomite process. 

    curl 'http://localhost:22222/info'

###Is this a fork from twemproxy?

 Yes, it is. When we started we realized that a bunch of work already there in twemproxy to get us progress quickly.  However, since our design is so different from twemproxy's design, we eventually had to do what is best for our project while still retaining all twemproxy's commit histories, its copyright notices and also other open sources' copyright notices that twemproxy uses, e.g. the FreeBSD code from University of Berkeley.


###Why is it written in C?
 We were evaluating the pros and cons between a JVM language (Java/Scala), Go, or C/C++.  A JVM language will force us to worry about GC and we don't have a deterministic control over it.  Go seems to be a new and interesting language. However, at the time we explored it, it still had long GC issues in some cases and plus, not many detailed documents available on the internet to give us the confidence.  C/C++ give us a total control we can get the best possible performance to avoid the bottleneck issue in our layer.  We finally settled on C as we don't need many features from C++ and if we do, converting a C project to a C++ project should be straight forward.

###Is there a sidecar to manage a Dynomite cluster (e.g. like Priam for Cassandra)?
Yes, we have open sourced [Dynomite-manager](https://github.com/netflix/dynomite-manager). Dynomite-manager provides discovery and healthcheck, Dynomite/Redis cold bootstrap (warm up), monitoring and insight integration, multi-region Dynomite deployment via public IP, automated security group updates, object storage backups (AWS S3).

###What client can we use to connect to Dynomite?
If you are using a JVM language, we have our Java client, Dyno, for you.
It has many advanced features and you can read more about it at http://github.com/Netflix/dyno.
Otherwise, you can use just any other Memcached or Redis client out there to connect but of course, you will not have those advanced features Dyno provide such as failover strategy, retries strategy, etc.