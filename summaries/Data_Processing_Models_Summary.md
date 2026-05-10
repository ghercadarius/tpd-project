# Data and Processing Models for Big Data: Summary

## 1. High Performance Computing (HPC) vs. Big Data
The document contrasts two distinct computing worlds [cite: 1409]:
* **High Performance Computing (HPC):** Traditionally focused on simulations and experiments run on supercomputers [cite: 1412, 1413]. Its ecosystem relies heavily on tools and languages like FORTRAN, C/C++, MPI/OpenMP, and parallel file systems (PFS) [cite: 1454, 1456, 1457].
* **Big Data:** Centered around commercial and scientific analytics executed on distributed cloud infrastructures [cite: 1417]. The Big Data ecosystem utilizes technologies like Hadoop (HDFS, Map-Reduce), Apache Spark, and NoSQL key-value stores like HBase [cite: 1444, 1448, 1449].
* Historically, the tools and cultures of these two domains diverged to their mutual detriment, highlighting a need for better integration [cite: 1432].

## 2. Big Data Processing Models
There are two primary paradigms for processing massive datasets [cite: 1478]:
* **Batch Processing:** Involves collecting a series of data over time and processing it all together as a single group or batch [cite: 1480, 1482, 1483]. This method yields exact results but operates with high latency [cite: 1512, 1515]. It is best suited for executing periodic queries against historical event data to build exact historical models [cite: 1523, 1524, 1525].
* **Real-time (Stream) Processing:** Data is processed immediately as it is collected, making results available almost instantaneously [cite: 1488, 1489]. This provides low-latency insights but often yields approximate results [cite: 1513, 1516]. It is used for continuous queries on real-time events to build approximate models [cite: 1528, 1531, 1532].
* **Lambda Architectures:** Represent the state-of-the-art approach by combining both batch processing (for exact historical correctness) and stream processing (for real-time speed) [cite: 1520].

## 3. Data Models and The CAP Theorem
Traditional relational database management systems (RDBMS) rely on ACID properties (Atomicity, Consistency, Isolation, Durability) [cite: 1539, 1540, 1542, 1544, 1546]. However, in a geographically distributed cloud environment with high-latency networks, providing strict ACID guarantees is highly expensive [cite: 1570, 1571, 1573, 1575, 1576]. 

This limitation is formally described by the **CAP Theorem**, which states that a distributed system cannot simultaneously provide all three of the following guarantees [cite: 1589, 1592]:
* **Consistency (C):** All nodes see the exact same data at the same time [cite: 1581].
* **Availability (A):** Every request receives a response, and node failures do not halt the surviving system [cite: 1582, 1583, 1584].
* **Partition Resilience (P):** The system continues operating despite network failures or message loss between nodes [cite: 1585, 1587, 1588].

Because scaling out inherently requires network distribution, **Partition Tolerance is a necessity, not an option** [cite: 1677]. Therefore, systems must dynamically trade-off between Consistency and Availability [cite: 1677, 1717]:
* **AP Systems (Forfeit strong Consistency):** Prioritize availability, offering "best effort" consistency [cite: 1646]. Examples include Cassandra, CouchDB, Riak, and DNS/web caching [cite: 1634, 1637, 1640].
* **CP Systems (Forfeit Availability):** Prioritize consistency, sometimes making minority network partitions unavailable [cite: 1660]. Examples include MongoDB, Redis, and distributed locking protocols [cite: 1648, 1649, 1654].
* **CA Systems (Forfeit Partitions):** Only possible in single-site databases like traditional MySQL or PostgreSQL installations [cite: 1665, 1666, 1668].

## 4. Consistency vs. Latency
The choice between consistency and availability is not strictly binary [cite: 1679]. Consistency comes in different degrees [cite: 1686]:
* **Strong Consistency:** Ensures that any access following an update will immediately return the new value [cite: 1687, 1688].
* **Eventual Consistency:** A form of weak consistency which guarantees that if no new updates occur, all accesses will *eventually* return the last updated value (e.g., through lazy replica propagation) [cite: 1689, 1690, 1691].

Ultimately, Availability and Latency are deeply interconnected [cite: 1770]. High availability requires data replication, and replication inherently requires consistency maintenance [cite: 1771, 1773, 1775]. Enforcing stronger consistency demands more network communication, which directly increases latency [cite: 1777]. Consequently, modern Big Data systems (like NoSQL databases) frequently give up strong consistency to minimize latency and ensure the system remains highly available [cite: 1756, 1758].
