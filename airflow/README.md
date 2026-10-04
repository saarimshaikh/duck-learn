# 🌬️ Apache Airflow Orchestration & Production Engineering

> *"Airflow is not an execution engine; it is a workflow orchestrator. The moment you start loading 10 GB of data into memory inside an Airflow PythonOperator, you have violated its core design philosophy and guaranteed scheduler instability."*  
> — **Professor's Field Note**

---

## 🏛️ Airflow Distributed Architecture Blueprint

```mermaid
flowchart TB
    subgraph ControlPlane["1. Airflow Control Plane"]
        direction TB
        Webserver["Airflow Webserver (Flask / Gunicorn - UI & REST API)"]
        Scheduler["Airflow Scheduler (Multi-threaded DAG Parser & State Machine)"]
        Triggerer["Airflow Triggerer (Asyncio Event Loop for Deferrable Operators)"]
        MetaDB[("Metadata Database<br/>(PostgreSQL + PgBouncer Pooling)")]
        
        Webserver <--> MetaDB
        Scheduler <--> MetaDB
        Triggerer <--> MetaDB
    end

    subgraph ExecutorLayer["2. Distributed Execution Layer"]
        direction TB
        subgraph CeleryExec["Celery Architecture"]
            Broker["Message Broker (Redis / RabbitMQ)"]
            Worker1["Celery Worker Node A"]
            Worker2["Celery Worker Node B"]
            Broker --> Worker1 & Worker2
        end

        subgraph K8sExec["Kubernetes Architecture"]
            K8sAPI["Kubernetes API Server"]
            Pod1["Ephemeral Task Pod 1"]
            Pod2["Ephemeral Task Pod 2"]
            K8sAPI --> Pod1 & Pod2
        end
    end

    subgraph RemoteStorage["3. Remote Logs & Custom XCom Storage"]
        S3["Cloud Object Store (S3 / ADLS / GCS Logs & Data Payloads)"]
    end

    Scheduler --> Broker
    Scheduler --> K8sAPI
    Worker1 & Worker2 <--> MetaDB
    Worker1 & Worker2 --> S3
    Pod1 & Pod2 --> S3
```

---

## 📖 Lecture Chapters

1. **[01. Airflow Architecture & Executor Internals](file:///d:/self-learning/airflow/01_airflow_architecture_and_components.md)**
   - Core daemons: Webserver, Scheduler, Triggerer, Metadata Database.
   - The Scheduler Heartbeat loop & `DagFileProcessorManager`.
   - Distributed Executors compared: `LocalExecutor`, `CeleryExecutor`, `KubernetesExecutor`, and `CeleryKubernetesExecutor`.

2. **[02. DAG Authoring & Scheduling Mechanics](file:///d:/self-learning/airflow/02_dag_authoring_and_scheduling_internals.md)**
   - Demystifying Time: `logical_date` (execution_date) vs `data_interval_start/end` vs `run_after`.
   - The Top-Level Code Trap: Why `import` and database queries outside tasks destroy scheduler performance.
   - TaskFlow API (`@task`, `@dag`) and dynamic DAG factory patterns.
   - Mastering `catchup`, `depends_on_past`, and distributed backfills.

3. **[03. Production Patterns & Deferrable Operators](file:///d:/self-learning/airflow/03_production_patterns_and_deferrable_operators.md)**
   - Smart Sensors vs Deferrable Operators (Freeing worker slots with `asyncio` Triggerers).
   - XCom Internals: Why default XComs destroy PostgreSQL databases, and configuring Custom Object Storage XCom backends (S3/GCS).
   - High-concurrency resource management: Pools, Priority Weights, and Slots.
   - Zombie task forensics and graceful heartbeat handling.

4. **[04. Airflow Interview Mastery](file:///d:/self-learning/airflow/04_interview_mastery_airflow.md)**
   - Debugging high Scheduler CPU and database connection starvation using PgBouncer.
   - Zero-Downtime DAG deployments: Git-Sync vs Immutable Docker images.
   - Safe historical backfills across millions of runs without destabilizing production queues.
