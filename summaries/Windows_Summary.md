# Stream Processing with Apache Flink: Windows and Time

## 1. Time in Streaming Engines
To effectively exceed the capabilities of batch systems, streaming engines require:
* **Correctness:** Strong consistency for exactly-once processing.
* **Tools for Reasoning about Time:** Mechanisms to deal with unbounded, unordered data and event-time skew [cite: 3].

### Event Time vs. Processing Time
* **Event Time:** The time an event actually occurred at the source. It provides consistent and deterministic results but incurs latency while waiting for out-of-order events. This is recommended for accurate analytics, time-based aggregations, and systems with variable latencies [cite: 5, 6].
* **Processing Time:** The system time of the machine executing the task. It requires no coordination (offering the lowest latency) but can yield incorrect results due to out-of-order events. It is best for simple applications where exact ordering is not critical [cite: 5, 6].
* In practice, there is a highly variable skew between event time and processing time due to network congestion, shared resources, or variable throughput. Thus, bounded pieces of data must be evaluated using **Windows** [cite: 7].

### Watermarks
* Watermarks are the mechanism used to measure progress in *event time*. They are injected into the data stream and carry a timestamp `t`.
* A `Watermark(t)` declares that no more elements with a timestamp `t' <= t` will arrive; any such elements are considered "late" [cite: 8].
* Flink uses a `TimestampAssigner` to extract event times and a `WatermarkStrategy` (e.g., `forBoundedOutOfOrderness` or `forMonotonousTimestamps`) to generate watermarks and handle out-of-order data [cite: 9, 10].

## 2. Flink Windows
Windows group events from an unbounded stream into finite chunks to perform computations (sum, average, count, etc.) [cite: 12].
* **Evaluation:** A window is triggered when the watermark is greater than or equal to the window's end time (e.g., Watermark `12:01:00` triggers the `12:00-12:01` window) [cite: 12].
* **Keyed vs. Non-Keyed:**
  * *Non-Keyed:* The window applies to the entire stream [cite: 13].
  * *Keyed:* The stream is partitioned via `.keyBy()`, and windowed computations are performed in parallel across multiple subtasks [cite: 13].
* **Typical Pattern:** `stream.keyBy(key).window(...).aggregate(...)` [cite: 14].

### Window Assigners
Window Assigners define how elements are mapped to windows [cite: 17].
* **Tumbling Windows:** Fixed-size, non-overlapping time windows (e.g., hourly). Different windows may contain a different number of events [cite: 18, 19].
* **Sliding Windows:** Fixed-size windows that overlap based on a "slide" parameter. An event can belong to multiple sliding windows [cite: 20, 21].
* **Session Windows:** Non-overlapping windows with no fixed start/end time. They close when a predefined gap of inactivity (no incoming elements) occurs [cite: 22, 23, 24].
* **Global Windows:** Assigns all elements with the same key to a single window that never closes automatically. It requires a custom trigger to be evaluated [cite: 26, 27].
* **Count Windows:** Windows defined by a specific number of events (e.g., `.countWindow(100)`) rather than time [cite: 25].

## 3. Joining in Flink
A **Window Join** combines the elements of two different streams that share a common key and lie within the same time window [cite: 31]. Keys from both streams must be explicitly matched to join the elements [cite: 32, 33].

## 4. Window Functions and Lifecycle
* **Process Functions:** e.g., `ProcessWindowFunction` or `ProcessAllWindowFunction`. These process events individually while giving access to the context, keyed state, processing/event time (`ctx.timestamp()`), and timers (`ctx.timerService()`) [cite: 35, 36, 37].
* **Window Triggers:** Determine *when* a window fires (e.g., event-time trigger, processing-time trigger, count-trigger) [cite: 38, 39].
* **Window Evictors:** Optionally remove specific elements from the window *before* the evaluation function runs (e.g., Time Evictor, Count Evictor, Delta Evictor) [cite: 38, 39].

## 5. Working with State in Flink
State is required when operations must remember information across multiple events (e.g., aggregating values over time or searching for event patterns). Flink makes state fault-tolerant via checkpoints [cite: 40].
* **Keyed State:** Scoped per key after a `.keyBy()` operation. It is automatically partitioned and is the most commonly used state. Forms include:
  * `ValueState<T>`: A single updatable value per key [cite: 42].
  * `ListState<T>`: A list of elements per key [cite: 42].
  * `ReducingState<T>`: An incrementally reduced value [cite: 42].
  * `MapState<UK, UV>`: A list of key-value mappings per key [cite: 42].
* **Operator State:** Scoped per operator instance (subtask) and not tied to keys [cite: 41].

### Fault Tolerance & Checkpointing
* Flink periodically takes persistent state snapshots (checkpoints) and stores them in a distributed file system [cite: 45].
* **At least once:** Records may be reflected multiple times in the state upon recovery [cite: 46].
* **Exactly once:** Uses consistent checkpoints (via barriers) and coordinated sinks (two-phase commit) to ensure records are reflected exactly once, even upon failure [cite: 46].
* **State Backends:** Handle state storage, such as `HashMapStateBackend` (in-memory) or `EmbeddedRocksDBStateBackend` (disk-based for large state) [cite: 47].

## 6. Table API & SQL
The `TableEnvironment` serves as the entry point for Flink's relational APIs [cite: 50].
* It handles registering tables, executing SQL queries, and registering user-defined functions [cite: 50].
* You can seamlessly convert between a `DataStream` and a `Table` using `StreamTableEnvironment` [cite: 52].
* Queries can be written using Flink SQL or the Table API. Both support continuous incremental streaming updates as well as specialized efficient batch runtime modes [cite: 51, 53].
