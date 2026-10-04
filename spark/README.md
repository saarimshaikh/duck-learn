# ⚡ Apache Spark Deep Dive & Performance Engineering

> *"Most engineers think Spark is fast because it's 'in-memory.' That is only 10% of the story. Spark is fast because of Project Tungsten's cache-aware CPU architectures, Catalyst's query plan optimization, and whole-stage code generation that bypasses the JVM virtual dispatch overhead entirely."*  
> — **Professor's Field Note**

---

## 🏛️ Spark Runtime & Engine Architecture Blueprint

```mermaid
flowchart TB
    subgraph DriverNode["Spark Driver Process"]
        direction TB
        AppCode["User Code (DataFrame / SQL)"] --> Catalyst["Catalyst Optimizer<br/>(Logical Plan -> Optimized Physical Plan)"]
        Catalyst --> Tungsten["Project Tungsten<br/>(Whole-Stage CodeGen)"]
        Tungsten --> DAGSched["DAGScheduler<br/>(Splits DAG into Stages at Shuffle Boundaries)"]
        DAGSched --> TaskSched["TaskScheduler<br/>(Creates TaskSets & Dispatches)"]
        TaskSched --> SchedBackend["SchedulerBackend<br/>(Communicates with YARN / K8s)"]
    end

    subgraph ClusterMgr["Cluster Manager (YARN / Kubernetes / Standalone)"]
        ResMgr["Resource Allocator"]
    end

    subgraph Executor1["Executor 1 (Worker Node A)"]
        direction TB
        T1["TaskRunner Thread 1"]
        T2["TaskRunner Thread 2"]
        MemMgr1["Unified Memory Manager<br/>[Execution Memory | Storage Memory]"]
        ShuffleSvc1["External Shuffle Service"]
    end

    subgraph Executor2["Executor 2 (Worker Node B)"]
        direction TB
        T3["TaskRunner Thread 3"]
        T4["TaskRunner Thread 4"]
        MemMgr2["Unified Memory Manager<br/>[Execution Memory | Storage Memory]"]
        ShuffleSvc2["External Shuffle Service"]
    end

    SchedBackend <--> ResMgr
    TaskSched -->|Dispatches TaskSets| Executor1
    TaskSched -->|Dispatches TaskSets| Executor2
    Executor1 <-->|Shuffle Exchanges (HTTP/Netty)| Executor2
```

---

## 📖 Lecture Chapters

1. **[01. Spark Core Engine Internals](file:///d:/self-learning/spark/01_spark_core_engine_internals.md)**
   - Driver vs Executor responsibilities.
   - The DAGScheduler: Lineage graphs, Stage boundaries, and Narrow vs Wide Dependencies.
   - The TaskScheduler and TaskRunner lifecycle.
   - Fault tolerance via RDD/DataFrame lineage reconstruction.

2. **[02. Memory Management & Project Tungsten](file:///d:/self-learning/spark/02_memory_management_and_tungsten.md)**
   - The Unified Memory Manager: Execution vs Storage vs User Memory vs Reserved Memory.
   - On-heap vs Off-heap (`sun.misc.Unsafe`) allocation.
   - The Catalyst Optimizer: Analysis, Logical Optimization, Physical Planning, and Cost-Based Optimizer (CBO).
   - Project Tungsten: Whole-Stage Code Generation (WSCG) and Cache-Aware Binary In-Memory Layout.

3. **[03. Shuffling, Partitioning & Skew Mitigation](file:///d:/self-learning/spark/03_shuffling_partitioning_and_skew.md)**
   - SortShuffleManager internals (Index and Data files).
   - Partition sizing: The 100 MB–200 MB sweet spot rule.
   - Adaptive Query Execution (AQE): Dynamic partition coalescing, runtime join switching, and automated skew join splitting.
   - Join strategies deep-dive: Broadcast Hash Join vs Sort-Merge Join vs Shuffle-Hash Join.
   - Manual skew resolution: Salting techniques with random prefixes.

4. **[04. Structured Streaming & Exactly-Once Semantics](file:///d:/self-learning/spark/04_structured_streaming_and_microbatch.md)**
   - Microbatch engine vs Continuous Processing Engine.
   - Event-time processing, Watermarking, and state eviction.
   - StateStore providers: Default HDFS-backed vs RocksDB StateStore.
   - End-to-end Exactly-Once guarantees: Replayable sources + StateStore checkpoints + Idempotent sinks.

5. **[05. Spark Interview Mastery](file:///d:/self-learning/spark/05_interview_mastery_spark.md)**
   - Diagnosing Driver OOM vs Executor OOM in production logs.
   - Production JVM Garbage Collection tuning (G1GC settings).
   - Sizing Spark jobs: The exact formula for executor memory, cores, and overhead.
   - Live architectural coding problems from Tier-1 tech interviews.
