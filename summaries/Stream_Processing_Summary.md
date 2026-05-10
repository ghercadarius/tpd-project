# Stream Processing with Apache Flink: Document Summary

## 1. Data Processing Paradigms
The document contrasts two primary data processing models:
* **Batch Processing:** Handles bounded (finite) datasets. It involves storing millions of records and processing the entire dataset later, making it suitable for analyzing historical data, identifying trends, and generating periodic reports. This approach inherently has a time delay (hours to days).
* **Unbounded (Stream) Data Processing:** Handles infinite datasets. Data is continuously processed as it arrives without waiting for it to accumulate. This "real-time processing" ensures high freshness of results, enabling fast decision-making, event monitoring, and tracking.

## 2. Requirements for Streaming Execution Engines
A well-designed streaming system offers a strict superset of batch processing functionality. It requires:
* **Correctness:** Strong consistency guarantees to ensure exactly-once processing.
* **Tools for Reasoning about Time:** Mechanisms to handle unbounded and unordered data, specifically addressing event-time skew.

## 3. Introduction to Apache Flink
Originating from TU Berlin, Apache Flink is a leading stream processing engine for real-time analytics in enterprise and cloud environments.
* **Key Features:**
  * Supports event time and out-of-order processing.
  * Uses checkpointing and state snapshots to recover from failures, ensuring exactly-once and at-least-once processing.
  * Facilitates large-scale stateful computations via RocksDB.
  * Offers flexible APIs: DataStream API (streaming), Table API & SQL (declarative), and Batch Processing.
  * Integrates with major data sources/sinks like Kafka, Kinesis, RabbitMQ, Elasticsearch, and HDFS.
  * Flexible deployment on Kubernetes, YARN, Mesos, standalone clusters, and local machines.

## 4. Flink Architecture and Parallelism
* **Dataflow Graph:** Represents a job's execution plan as a Directed Acyclic Graph (DAG) containing Source, Transformation, and Sink operators.
* **Core Components:**
  * **JobManager (Master):** Parallelizes jobs, distributes task slices to TaskManagers, and coordinates checkpoints/recovery.
  * **TaskManagers (Workers):** Execute operator subtasks and exchange data streams.
* **Parallelism Strategies:**
  * *Stream Partitions:* Divide data across parallel downstream tasks using strategies like KeyBy (hash partitioning), Rebalance (round-robin), Shuffle (random), Broadcast, and Rescale.
  * *Operator Subtasks:* Parallel instances of an operator running on worker slots, independently maintaining state and processing input partitions.

## 5. Data Stream Transformations
* **Stateless Transformations:** Act on single events independently.
  * *Filter:* Evaluates a condition to retain or discard events.
  * *Map:* Transforms exactly one input event into one output event.
  * *FlatMap:* Transforms one input event into 0, 1, or multiple output events.
* **Stateful Transformations:**
  * *KeyBy:* Logically partitions a stream so all records with the same key are assigned to the same downstream task. It is a prerequisite for grouped aggregations.
  * *Aggregations:* Functions like `sum()`, `min()`, `max()`, and `reduce()` update running totals or records for grouped streams.

## 6. Operations on Multiple Streams
Flink provides several operators to merge or process multiple streams simultaneously:
* **Union:** Combines two or more streams of the *same* data type. The order of records is not guaranteed.
* **Connect:** "Connects" two streams of *different* data types while retaining their respective types.
* **CoMap / CoFlatMap:** Applied to connected streams to process two streams together while running different mapping logic on each, ultimately outputting a single unified stream type.

## 7. Submitting and Managing Flink Jobs
* **Deployment Steps:**
  1. Package the application logic into a JAR file.
  2. Use the Flink CLI, REST API, or Web UI to submit the job.
  3. Monitor execution and manage checkpoints via the Flink Job UI.
* **Execution Modes:**
  * *Local Execution:* Runs directly from an IDE or CLI using `createLocalEnvironment()`.
  * *Cluster Execution:* Submitted via CLI (using the `-m` flag) to Standalone Clusters, YARN (Session or Per-job mode), or Kubernetes.
