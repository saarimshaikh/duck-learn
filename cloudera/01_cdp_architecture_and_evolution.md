# 🏛️ Lecture 01: CDP Architecture & Evolution
### *From CDH & HDP to the Modern Cloudera Data Platform*

---

## 1. The Great Enterprise Merger: CDH + HDP = CDP

For a decade, the Big Data enterprise landscape was divided between two titans: **Cloudera (CDH)** and **Hortonworks (HDP)**. In 2018, the companies merged to unify their competing technologies into a single flagship platform: **Cloudera Data Platform (CDP 7.x+)**.

```mermaid
flowchart TD
    subgraph CDH["Legacy Cloudera (CDH)"]
        CM["Cloudera Manager"]
        Impala["Apache Impala"]
        Kudu["Apache Kudu"]
        Parcels["Parcel Packaging Model"]
    end

    subgraph HDP["Legacy Hortonworks (HDP)"]
        Ranger["Apache Ranger (Security/Masking)"]
        Atlas["Apache Atlas (Lineage/Catalog)"]
        Knox["Apache Knox (Gateway/SSO)"]
        LLAP["Apache Hive 3 & LLAP"]
    end

    subgraph CDP["Cloudera Data Platform (CDP 7.x+)"]
        SDX["SDX (Shared Data Experience) = Ranger + Atlas + Knox"]
        CoreCompute["Compute = Spark + Impala + Hive LLAP"]
        CoreMgmt["Management = Cloudera Manager + Parcels"]
    end

    CDH --> CDP
    HDP --> CDP
```

### The Best-of-Breed Technology Decisions

| Domain | Legacy CDH Choice | Legacy HDP Choice | CDP Unified Choice | Architectural Reason |
| :--- | :--- | :--- | :--- | :--- |
| **Management** | Cloudera Manager | Apache Ambari | **Cloudera Manager** | Superior enterprise HA, parcel management, and automated rolling upgrades. |
| **Authorization** | Apache Sentry | Apache Ranger | **Apache Ranger** | Ranger offered tag-based policies, dynamic row filtering, and column masking. |
| **Governance** | Cloudera Navigator | Apache Atlas | **Apache Atlas** | Atlas provided extensible open APIs and native Kafka hook integration. |
| **Interactive SQL**| Apache Impala | Hive LLAP | **Both Supported** | Impala for ultra-low latency BI; Hive LLAP for heavy federated SQL & ETL. |

---

## 2. CDP Deployment Flavors: Private vs. Public Cloud

```mermaid
flowchart TB
    subgraph CDPPrivateCloudBase["1. CDP Private Cloud Base (Bare-Metal / Traditional)"]
        direction TB
        BareNodes["Physical Rack Servers / SAN / JBOD"]
        Storage1["HDFS / Apache Ozone Distributed Storage"]
        Compute1["YARN / Spark / Impala / Hive LLAP"]
        SDX1["SDX Security & Governance (Ranger + Atlas)"]
        CM1["Cloudera Manager Control Plane"]
        
        BareNodes --> Storage1 --> Compute1 <--> SDX1
        CM1 -. Manages .-> Compute1
    end

    subgraph CDPPrivateCloudDS["2. CDP Private Cloud Data Services (Cloud-Native K8s)"]
        direction TB
        OCP["Red Hat OpenShift / Enterprise Kubernetes"]
        CDE["CDE: Cloudera Data Engineering (Spark on K8s)"]
        CDW["CDW: Cloudera Data Warehouse (Auto-scaling Impala pods)"]
        CML["CML: Cloudera Machine Learning (Jupyter/Ray on GPU pods)"]
        
        OCP --> CDE & CDW & CML
    end

    CDPPrivateCloudBase ==>|Shared Storage & SDX Security Control| CDPPrivateCloudDS
```

1. **CDP Private Cloud Base**:
   - The foundation layer deployed directly on physical bare-metal servers.
   - Hosts persistent storage (HDFS / Apache Ozone) and classic YARN execution engines.
2. **CDP Private Cloud Data Services**:
   - Deployed on **Red Hat OpenShift (Kubernetes)**.
   - Decouples compute from storage: Spawns ephemeral, auto-scaling containers for Spark (CDE), Impala (CDW), and ML (CML) that read from the Base cluster's storage while sharing the exact same Ranger security policies.
3. **CDP Public Cloud**:
   - Fully managed control plane across AWS, Azure, and Google Cloud.
   - Replaces HDFS with cloud object stores (S3, ADLS Gen2, GCS) while keeping SDX security policies uniform across on-prem and multi-cloud environments.

---

## 3. Cloudera Manager (CM) Internals: Under the Hood

Cloudera Manager is the brain that automates the deployment, configuration, monitoring, and rolling upgrades of hundreds of distributed service daemons.

```mermaid
sequenceDiagram
    autonumber
    participant CM_Server as Cloudera Manager Server (Port 7180)
    participant DB as Backend Database (PostgreSQL / MySQL)
    participant Agent as Cloudera Manager Agent (Port 9000)
    participant Super as Supervisord Daemon
    participant Service as Service Process (e.g. Impalad / DataNode)

    Note over CM_Server,DB: Administrator updates configuration in UI
    CM_Server->>DB: Persist new config to PostgreSQL
    
    loop Every 15 Seconds (Heartbeat Loop)
        Agent->>CM_Server: Heartbeat POST (Host health, disk space, CPU load)
        CM_Server-->>Agent: Heartbeat ACK (New configuration generation #42)
    end

    Agent->>Agent: Generate config files in /var/run/cloudera-scm-agent/process/42-impalad/
    Agent->>Super: Command: Graceful restart of impalad
    Super->>Service: Send SIGTERM, wait for drain, restart process
    Service-->>Super: Process started (PID 18420)
    Super-->>Agent: Process running healthy
    Agent->>CM_Server: Report State: READY
```

### Why Parcels Win Over OS Packages (RPMs/DEBs)
Legacy systems used standard Linux RPMs to install Hadoop. If you upgraded an RPM, it immediately altered system libraries in `/usr/lib`, making rollbacks almost impossible without cluster downtime.

**The Parcel Architecture**:
- A **Parcel** is a self-contained tarball containing binaries, libraries, and metadata for a service suite.
- Parcels are downloaded to `/opt/cloudera/parcel-repo` and unpacked into versioned directories:
  `/opt/cloudera/parcels/CDH-7.1.8-1.cdh7.1.8.p0.30990546/`
- When you "Activate" a parcel, Cloudera Manager simply swaps symbolic links (`/opt/cloudera/parcels/CDH -> ...`).
- **Advantage**: If a rolling upgrade encounters a critical bug, rolling back is instantaneous—CM simply repoints the symlink to the previous parcel version without modifying the host OS!
