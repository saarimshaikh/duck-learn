# ⚙️ Lecture 01: Airflow Architecture & Executor Internals
### *The Scheduler Engine, Triggerer, and Distributed Worker Topologies*

---

## 1. Airflow Core Architecture & Component Interactions

```mermaid
flowchart TB
    subgraph Storage["Persistent Layer"]
        DAGFolder["DAG Directory (/dags)"]
        MetaDB[("Metadata Database (PostgreSQL)")]
    end

    subgraph SchedulerProcess["The Scheduler Daemon"]
        direction TB
        FileProcessor["DagFileProcessorManager<br/>(Subprocess pool parsing Python AST)"]
        SerializedDAG["Serialized DAG Store"]
        SchedLoop["Scheduler Loop<br/>(Dependency evaluation & State Machine)"]
        
        FileProcessor --> SerializedDAG
        SchedLoop <--> MetaDB
    end

    subgraph AsyncEngine["Asynchronous Trigger Engine"]
        Triggerer["Triggerer Daemon<br/>(Single Python asyncio Event Loop)"]
        Triggerer <--> MetaDB
    end

    subgraph ExecutionPlane["Distributed Execution Options"]
        CeleryWorker["Celery Workers (Redis Queue)"]
        K8sPods["Kubernetes Pods (K8s API)"]
    end

    DAGFolder --> FileProcessor
    SerializedDAG --> MetaDB
    SchedLoop -->|Pushes Queued Tasks| CeleryWorker
    SchedLoop -->|Spawns Ephemeral Pods| K8sPods
```

### The 4 Essential Daemons
1. **Webserver**: Lightweight UI serving status, logs, and manual triggers. In Airflow 2.x+, the webserver reads strictly from the database's `serialized_dag` table; it **never** parses DAG Python files directly, eliminating UI freezes.
2. **Scheduler**: The heart of Airflow. A multi-threaded process running a continuous loop:
   - Spawns background subprocesses to parse Python files into serialized representations.
   - Evaluates dependency state machines (`NONE` $\rightarrow$ `SCHEDULED` $\rightarrow$ `QUEUED` $\rightarrow$ `RUNNING`).
   - Delegates queued tasks to the Executor.
3. **Triggerer**: An event-loop daemon introduced to solve sensor starvation. Runs thousands of asynchronous waiting events in a single thread using Python `asyncio`.
4. **Metadata Database**: The single source of truth (PostgreSQL / MySQL) recording task states, connection secrets, DAG runs, and execution timestamps.

---

## 2. Distributed Executors Compared

Choosing the correct executor is one of the most critical architectural decisions for a DataOps platform.

```mermaid
flowchart TD
    subgraph CeleryExec["CeleryExecutor (Pre-warmed Worker Fleet)"]
        Sched1["Scheduler"] -->|Tasks| Redis["Redis / RabbitMQ"]
        Redis --> W1["Worker 1 (Fixed 16 Slots)"]
        Redis --> W2["Worker 2 (Fixed 16 Slots)"]
    end

    subgraph K8sExec["KubernetesExecutor (Pod-per-Task)"]
        Sched2["Scheduler"] -->|API Call| K8s["K8s API Server"]
        K8s -->|Spawns| P1["Pod: Task A (2 CPU, 4 GB)"]
        K8s -->|Spawns| P2["Pod: Task B (8 CPU, 32 GB + GPU)"]
    end
```

### The Definitive Executor Tradeoff Matrix

| Feature | CeleryExecutor | KubernetesExecutor | CeleryKubernetesExecutor |
| :--- | :--- | :--- | :--- |
| **Worker Model** | Long-running, pre-warmed worker VMs/containers | Ephemeral: 1 dedicated Pod spawned per task | Hybrid: Pre-warmed fleet + on-demand Pods |
| **Task Startup Latency** | **Sub-second (< 200 ms)** | **5–15 seconds** (Container image pull & Pod schedule) | Fast for Celery; 5–15s for K8s |
| **Dependency Isolation** | Poor (All tasks share worker Python virtualenv) | **Total** (Each task can specify a unique Docker image) | Isolated on K8s queue; shared on Celery |
| **Resource Granularity** | Static (Workers sized uniformly) | **Dynamic** (Specify exact CPU/RAM/GPU per task) | Best of both worlds |
| **Infrastructure Cost** | Pays for idle workers 24/7 | **Scales to Zero** when no tasks are running | Optimized: small Celery baseline + K8s burst |
| **Best For** | High-throughput, short-running tasks (SQL triggers, sensors) | Heavy, heterogeneous data pipelines (Spark, ML, PyTorch) | **Large Enterprise Data Platforms** |

---

## 3. The Lifecycle of a Task Instance

Every task instance transitions through a deterministic state machine managed by the Scheduler:

```mermaid
stateDiagram-v2
    [*] --> None
    None --> Scheduled: Dependencies Met (Upstream Tasks Success)
    Scheduled --> Queued: Sent to Executor Queue
    Queued --> Running: Picked up by Worker / Pod
    Running --> Success: Task exits with code 0
    Running --> Failed: Exception or Non-zero Exit
    Running --> UpForRetry: Task fails, retries remaining
    UpForRetry --> Scheduled: Retry delay elapsed
    Running --> Deferred: Deferrable Operator yields Trigger
    Deferred --> Scheduled: Trigger fires event
    Success --> [*]
    Failed --> [*]
```

### Preventing Zombie Tasks
A **Zombie Task** is a task instance that the metadata database thinks is in `RUNNING` status, but whose underlying worker thread or Kubernetes pod has crashed silently (e.g., node died, or pod was evicted by K8s OOMKiller).

The Airflow Scheduler runs a periodic zombie detection loop:
- Workers continuously send heartbeat timestamps to the DB (`task_instance.heartbeat`).
- If a task instance in `RUNNING` status has not updated its heartbeat within `scheduler.zombie_task_threshold` (default **300 seconds / 5 mins**), the Scheduler marks the task as `FAILED`, frees its slot, and triggers retry logic.
