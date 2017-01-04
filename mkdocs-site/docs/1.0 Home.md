<p align="center">
  <img src="/Netflix/dynomite/wiki/images/dynomite-logo.png" />
</p>
Welcome to the Dynomite wiki!  

Dynomite is a generic [dynamo](http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) implementation that can be used with many different key-value pair storage engines. Currently these include [Redis](http://redis.io) and [Memcached](http://www.memcached.org/).  Dynomite supports multi-datacenter replication and is designed for high availability.

The ultimate goal with Dynomite is to be able to implement high availability and cross-datacenter replication on storage engines that do not inherently provide that functionality. The implementation is efficient, not complex (few moving parts), and highly performant.