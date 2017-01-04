About a year ago the Cloud Database Engineering (CDE) team published a post [Introducing Dynomite](http://techblog.netflix.com/2014/11/introducing-dynomite.html). Dynomite is a proxy layer that provides sharding and replication and can turn existing non-distributed datastores into a fully distributed system with multi-region replication. One of its core features is the ability to scale a data store linearly to meet rapidly increasing traffic demands. Dynomite also provides high availability, and was designed and built to support [Active-Active Multi-Regional Resiliency](http://techblog.netflix.com/2013/12/active-active-for-multi-regional.html). 
Dynomite, with [Redis](http://redis.io/) is now utilized as a production system within Netflix. This post is the first part of a series that pragmatically examines Dynomite's use cases and features. In this post, we will show performance results using Amazon Web Services (AWS) with and without the recently added consistency feature. 

## Dynomite Consistency
Dynomite extends eventual consistency to [tunable consistency](https://github.com/Netflix/dynomite/wiki/Consistency) in the local region. The consistency level specifies how many replicas must respond to a write/read request before returning data to the client application. Read and write consistency can be configured to manage availability versus data accuracy. Consistency can be configured for read or write operations separately (cluster-wide). There are two configurations:

**DC_ONE**:
Reads and writes are propagated synchronously only to the node in the local rack (Availability Zone) and asynchronously replicated to other Racks and regions.

**DC_QUORUM**:
Reads and writes are propagated synchronously to quorum number of nodes in the local data center and asynchronously to the rest. The DC_QUORUM configuration writes to the number of nodes that make up a quorum. A quorum is calculated, and then rounded up to a whole number. The operation succeeds if the read/write succeeded on a quorum number of nodes.

## Test Setup
For the workload generator, we used an internal Netflix tool called Pappy. Pappy is well integrated with other Netflix OSS services such as (Archaius for fast properties, Servo for metrics, and Eureka for discovery). However, any other other distributed load generator with Redis client plugin can be used to replicate the results. Pappy has support for modules, and one of them is Dyno Java client.

Dyno client uses topology aware load balancing (Token Aware) to directly connect to a Dynomite coordinator node that is the owner of the specified data. Dyno also uses zone awareness to send traffic to Dynomite nodes in the local ASG. To get full benefit of a Dynomite cluster a) the Dyno client cluster should be deployed across all ASGs, so all nodes can receive client traffic, and b) the number of client application nodes per ASG must be larger than the corresponding number of Dynomite nodes in the respective ASG so that the cumulative network capacity of the client cluster is at least equal to the corresponding one at the Dynomite layer. 

Dyno also uses connection pooling for persistent connections to reduce the connection churn to the Dynomite nodes. However, in performance benchmarks tuning Dyno can tricky as the workload generator make become the bottleneck due to thread contention. In our benchmark, we observed the delay metrics to pick up a connection from the connection pool that Dyno exposes.

Client (workload generator) Cluster
* Client: Dyno Java client, using default configuration (token aware + zone aware) 
* Number of nodes: Equal to the number of Dynomite nodes in each experiment.
* Region: us-west-2 (us-west-2a, us-west-2b and us-west-2c)
* EC2 instance type: m3.2xlarge (30GB RAM, 8 CPU cores, Network throughput: high)
* Platform: EC2-Classic
* Data size: 1024 Bytes
* Number of Keys: 1M random keys
* Demo application used a simple workload of just key value pairs for read and writes i.e the Redis GET and SET api.
* Read/Write ratio: 80:20 (the OPS was variable per test, but the ratio was kept 80:20)
Number of readers/writers: 80:20 ratio of reader to writer threads. 32 readers/8 writers  per Dynomite Node. We performed some experiments varying the number of readers and writers and found that in the context of our experiments, 32 readers and 8 writes per dynomite node gave the best throughput latency tradeoff.


Dynomite Cluster
* Dynomite: Dynomite 0.5.6
* Data store: Redis 2.8.9
* Operating system: Ubuntu 14.04.2 LTS
* Number of nodes: 3-48 (doubling every time)
* Region: us-west-2 (us-west-2a, us-west-2b and us-west-2c)
* EC2 instance type: r3.2xlarge (61GB RAM, 8 CPU cores, Network throughput: high)
* Platform: EC2-Classic

The test was performed in a single region with 3 availability zones. Note that replicating to two other availability zones is not a requirement for Dynomite, but rather a deployment choice for high availability at Netflix. A Dynomite cluster of 3 nodes means that there was 1 node per availability zone. However all three nodes take client traffic as well as replication traffic from peer nodes. Our results were captured using [Atlas](https://github.com/Netflix/atlas). Each experiment was run 3 times, and our results are averaged over based on average of these times. Each run lasted 3h.

For the our benchmarks, we refer to the Dynomite node, as the node that contains the Dynomite layer and Redis. Hence we do not distinguish on whether Dynomite layer or Redis contributes to the average latency. 


## Linear Scale Test with DC_ONE

<!-- 
![Dynomite speed under different workloads](https://cloud.githubusercontent.com/assets/12148455/10854660/567b170c-7efa-11e5-97a2-0837cb143bd6.png)
-->
![Dynomite Throughput With DC_ONE](https://cloud.githubusercontent.com/assets/12148455/11127460/4d43ec5c-8929-11e5-8a83-7e3682072ad4.png)

The throughput graphs indicate that Dynomite can scale horizontally in terms of throughput. Therefore, it can handle even more traffic by increasing the number of nodes per region.

Moreover, on a per node basis with r3.2xlarge nodes, Dynomite can roughly process 33K reads OPS and around 10K write OPS concurrently from the client. This is because the traffic coming to each node is replicated to two other availability zones, therefore dribbling the effective throughput on a per node basis. Note that replicating to two other availability zones is not requirement for Dynomite, but rather a deployment choice at Netflix. For 1KB payload, as the graphs show the main bottleneck is the network (1Gbps EC2 instances). Therefore, switching to 2Gbps r3.4xlarge instances or 10Gbps capable EC2 instances, Dynomite can potentially provide even faster throughput. Note though that 10Gbps optimizations will only be effective when the instances are launched in Amazon's VPC with instance types that can support [Enhanced Networking using single root I/O virtualization (SR-IOV)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/enhanced-networking.html). 

The actual provisioning formula is also affected by the design of the client application. For example, to get full benefit of a Dynomite cluster the number of client nodes on a per availability zone at the client application must be larger than the corresponding ones on Dynomite. This will ensure that the client traffic is evenly distributed across the Dynomite cluster. Moreover, one also has to take into account the thread locking on the client side and the number of connections made to each Dynomite node. These can be observed at the Dyno client by looking into the delay to pick up a connection from the connection pool.

![Average & Median Latency](https://cloud.githubusercontent.com/assets/12148455/10853789/35fbfdca-7ef5-11e5-970b-22caad4a807b.png)

For the above results, we refer to the Dynomite node, as the node that contains the Dynomite layer and Redis.  Hence we do not distinguish on whether Dynomite layer or Redis contributes to the average latency. The average and median latency values show that Dynomite can provide sub-millisecond latency to the client application. More specifically, Dynomite does not add an extra latency as it scales to higher number of nodes, and therefore higher throughput. Overall, the Dynomite node adds around 20% of the average latency, and the rest of it is a result of the network latency. 

![95th Percentile Latency](https://cloud.githubusercontent.com/assets/12148455/10833886/58dc785e-7e52-11e5-9bef-25adc25ad31a.png)

At the 95th percentile Dynomite's latency is 0.4ms and does not increase as we scale the cluster up/down. More specifically, the network is the major reason for the 95th percentile latency, as Dynomite's node effect is <10%. 

![99th Percentile Latency](https://cloud.githubusercontent.com/assets/12148455/10833902/745ebd9e-7e52-11e5-8a90-1ad2518e25fd.png)

It is evident from the 99th percentile graph where the latency for Dynomite pretty much remains then same while the client side increases indicating the variable nature of the network between the clusters.

## Linear Scale Test With DC_QUORUM
The test setup was similar to what we used for DC_ONE tests above. Consistency was set to DC_QUORUM for both writes and reads on all Dynomite nodes.

<!-- ![Dynomite Speed With Consistency](https://cloud.githubusercontent.com/assets/12148455/10854365/7f13846c-7ef8-11e5-8531-d6ce079af74a.png)
-->

![Dynomite Throughput With DC_QUORUM](https://cloud.githubusercontent.com/assets/12148455/11127440/302d68d2-8929-11e5-93fb-6f35b7dd6f23.png)

Looking at the graph it is clear that dynomite still scales well as the cluster nodes are increasing. Moreover Dynomite node achieves 15K IOPS per node in our setup, when the cluster spans a single region. 

![Average & Median Latency With Consistency](https://cloud.githubusercontent.com/assets/12148455/10858999/6e728fd6-7f16-11e5-84ac-ddd39eb29ce8.png)

The average and median latency remains <3ms even when DC_QUORUM consistency is enabled in Dynomite nodes. Evidently, the average and median latency are slightly higher than the corresponding experiments with DC_ONE. This because the latency includes one to two hops to other availability zones and the corresponding operations performed on those nodes.

![95th Percentile Latency With Consistency](https://cloud.githubusercontent.com/assets/12148455/10859023/a3ce6f24-7f16-11e5-8c33-47cf0a01c691.png)
The 95th percentile at the Dynomite level is less 2.5ms regardless of the traffic sent to the cluster (linear scale), and at the client side it is below 4ms.

![99th Percentile Latency With Consistency](https://cloud.githubusercontent.com/assets/12148455/10859046/d6081e7c-7f16-11e5-8686-359218d24db4.png)

At the 99th Percentile with DC_QUORUM enabled, Dynomite produces less than 4ms of latency. When considering the network from the cluster to the client, the latency remains well below 10ms opening the door for a number of applications that require consistency with low latency across the board of percentiles.

## Pipelining
[Pipelining](http://redis.io/topics/pipelining) is client side batching; the client sends requests without waiting for a response from a previous request and later reads response for the whole batch. This way, the overall throughput and client side latency can increase. It is different from a transaction where all or none operations succeed. In pipelining, individual operations can succeed or fail. In the following experiments, the Dyno client randomly selected between 3 to 10 operations in one pipeline request. The experiments were performed for both DC_ONE and DC_QUORUM. 

![Dynomite Pipeline Throughput With DC_ONE](https://cloud.githubusercontent.com/assets/12148455/11127491/6c4a8b10-8929-11e5-94a0-8e46913749e7.png)

![Dynomite Pipeline Throughput With DC_QUORUM](https://cloud.githubusercontent.com/assets/12148455/11127511/84eee012-8929-11e5-8122-60f25b2db21c.png)

For comparison reason, we showcase both the non-pipelining and pipelining results. Evidently, pipelining increases the throughput by 20-50%. For a small Dynomite cluster the improvement is larger, but as Dynomite horizontally scales the benefit of pipeline decreases.

Latency is really a factor of how many requests are clubbed into one pipeline request so it will vary and will be higher than non pipeline requests.

## Conclusion

We performed the tests to get some more insights about Dynomite using Redis at the data store layer, and how to size our clusters. We could have achieved better results with better instance types both at the client and Dyomite server cluster. For example, adding Dynomite nodes with better network capacity (especially the ones supporting enhanced Networking on Linux Instances in a VPC) could further increase the performance of our clusters.
Another way to improve the performance is by using fewer availability zones. In that case,  Dynomite would replicate the data in one more availability zone instead of two more, hence more bandwidth would have been available to client connections. In our experiment we used 3 availability zones in us-west-2, which is a common deployment in most production clusters at Netflix. 

In summary, our benchmarks were based on instance types and deployments that are common at Netflix and Dynomite. We presented results that indicate that DC_QUORUM provides better read and write guarantees to the client but with higher latencies and lower throughput. We also showcased how a client can configure Redis Pipeline and benefit from request batching.

We briefly mentioned the availability of higher consistency in this article. In the next article we'll dive deeper into how we implemented higher consistency and how we handle anti-entropy.


<!-- ![Dynomite average latency under different workloads](https://cloud.githubusercontent.com/assets/4562887/9863859/ba5158a2-5af8-11e5-867d-13a8087798bf.png) 
![Dynomite 99th percentile latency under different workloads](https://cloud.githubusercontent.com/assets/4562887/9863861/bc9349ae-5af8-11e5-8e96-ec6377c31826.png) -->