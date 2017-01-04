A client can connect to any node on a Dynomite cluster when sending write traffic.  If the Dynomite node happens to own the data based on its token, then the data is written to the local datastore server process and asynchronously replicated to other racks in the cluster across all data centers.  If the node does not own the data, it acts as a coordinator and sends the write to the node owning the data in the same rack. It also replicates the writes to the corresponding nodes in other racks and DCs.

The diagram below shows an example where data is being written to the cluster by the client. It belongs on nodes a2,b2,c2 and d2 as per the partitioning scheme. The request is sent to a1 which acts as the coordinator and sends the request to the appropriate datastore nodes.

[[images/replication1.png]]

Dynomite features a regular, consistent hash ring. Replication is asymmetric. When a key is hashed to the ring, its owner is the node proceeding it in the ring. As shown in the graphic below, key 30 belongs to node 1 and key 200 belongs to node 4.    
   
[[images/paper-image00.png]]  
   
Local writes with the Dyno client employ *token aware* load balancing. The Dyno client is aware of the cluster topology of Dynomite within the same region, and hence can write directly to a specific node using consistent hashing.  
   
[[images/paper-image03.png]]  
   
Below, the Dyno client does a local write only (i.e local region) and the dynomite co-ordinator know its corresponding replica in the remote region and forwards on the write.

[[images/paper-image02.png]]  
  
Asymmetric replication looks slightly different. At some point in the future we could have another region where the capacity is different from the current regions. For example, assume that we have m2.4xl instances in us-east-1 and we have a 6 node token ring in vus-east-1. Then assume that in eu-west-1 we have only m2.2xl instances, hence we have a 12 node token ring in eu-west-1. 

In this scenario, key 30 goes to node  1 in us-east-1 but it goes to node 2 in eu-west-1. The Dyno client in each region is aware of the cluster topology and Dynomite is aware of all the topologies for remote regions (via gossip). By design, the client and server and respectively route the write to the correct set of nodes in both regions.  
   
[[images/paper-image01.png]]