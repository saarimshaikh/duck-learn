# ⚙️ Lecture 01: Kafka Internals & Broker Architecture
### *The Commit Log, Zero-Copy Syscalls, ISR Consensus & Tiered Storage*

---

## 1. The Append-Only Commit Log & Sparse Indexes

In Apache Kafka, a **Topic** is a logical category, and a **Partition** is the physical unit of parallelism and storage. A partition is stored on disk as an **ordered, immutable sequence of records** broken into **Log Segments**.

```mermaid
flowchart TB
    subgraph PartitionDirectory["Disk Directory: /var/lib/kafka/data/orders-0/"]
        direction TB
        Seg1["Segment 0: Active / Rolled<br/>00000000000000000000.log<br/>00000000000000000000.index<br/>00000000000000000000.timeindex"]
        Seg2["Segment 1: Active Append Head<br/>00000000000000100000.log<br/>00000000000000100000.index<br/>00000000000000100000.timeindex"]
    end

    subgraph SparseIndexLookup["Sparse Index (.index) Binary Search"]
        direction LR
        Idx["Offset 100000 -> Byte 0<br/>Offset 100040 -> Byte 4096<br/>Offset 100080 -> Byte 8192"]
        DataLog["Raw Data Log (.log)"]
        Idx -->|Direct Byte Seek| DataLog
    end
```

### The Sparse Index Mechanism
- Storing an index entry for every single message would consume excessive RAM.
- Kafka uses a **Sparse Memory-Mapped Index** (`.index`): It writes an index entry only every $N$ bytes (configured by `index.interval.bytes`, default **4096 bytes / 4 KB**).
- To read offset `100065`:
  1. Kafka executes an in-memory **binary search** on the `.index` file to find the largest indexed offset $\le 100065$ (finding `100040` at Byte `4096`).
  2. It performs a fast sequential disk read starting at byte `4096` in the `.log` file, scanning forward until it hits offset `100065`.

---

## 2. Why Kafka is Blazingly Fast: Page Cache & Zero-Copy

Engineers often ask: *"How can a Java application written on the JVM saturate a 40 Gbps network interface writing to spinning disks?"*

Kafka achieves this through two hardware-level optimizations:

```mermaid
flowchart TD
    subgraph TraditionalRead["❌ Traditional Application I/O (4 Context Switches, 4 Copies)"]
        Disk1["Storage Disk"] -->|1. DMA Copy| PageCache1["OS Page Cache"]
        PageCache1 -->|2. CPU Copy| JVMHeap["JVM User Memory Space"]
        JVMHeap -->|3. CPU Copy| SocketBuf["OS Socket Buffer"]
        SocketBuf -->|4. DMA Copy| NIC1["Network Interface Card (NIC)"]
    end

    subgraph ZeroCopyRead["✅ Kafka Zero-Copy: sendfile() Syscall (2 Context Switches, ZERO CPU Copies!)"]
        Disk2["Storage Disk"] -->|1. DMA Copy| PageCache2["OS Page Cache (Kernel Space)"]
        PageCache2 -. Direct DMA Transfer .-> NIC2["Network Interface Card (NIC)"]
        Note1["Bypasses JVM user memory completely!<br/>Zero GC overhead, Zero CPU memory copy."]
    end
```

1. **Avoid JVM Heap for Data**:
   - Storing gigabytes of data in Java heap introduces massive object overhead and long Garbage Collection pauses.
   - Kafka keeps the JVM heap small (typically 6–8 GB) and lets the **Linux OS Page Cache** manage all message data in physical RAM.
2. **Zero-Copy Network Transfer (`sendfile()` Syscall)**:
   - When a consumer reads data, Kafka invokes the Linux `sendfile()` system call.
   - The OS kernel copies data directly from the OS page cache to the Network Interface Card (NIC) buffer via **Direct Memory Access (DMA)**, completely bypassing JVM user-space memory!

---

## 3. High Availability: ISR, LEO & High Watermark

Every partition has one **Leader Broker** and zero or more **Follower Brokers**.

```mermaid
flowchart TB
    subgraph ISR_Group["Partition 0 (Replication Factor = 3)"]
        direction TB
        Leader["Broker 1: Leader<br/>LEO = 100 | HW = 98"]
        Follower1["Broker 2: Follower (In-Sync)<br/>LEO = 98"]
        Follower2["Broker 3: Follower (Lagging / Out-of-Sync)<br/>LEO = 80"]
    end

    Client["Consumer Group"] -->|Reads up to High Watermark ONLY| Leader
    Leader -->|Pulls Data| Follower1
    Leader -. Lagging: Evicted from ISR .-> Follower2
```

### Essential Architectural Definitions
- **Log End Offset (LEO)**: The offset of the next record to be written into a broker's local partition log.
- **In-Sync Replicas (ISR)**: The set of followers currently caught up with the leader within `replica.lag.time.max.ms` (default **30,000 ms / 30 seconds**).
- **High Watermark (HW)**: The minimum LEO across all replicas in the ISR.
  $$\mathbf{HW = \min_{r \in \text{ISR}}(\text{LEO}_r)}$$
- **Consumer Read Safety**: Consumers are strictly forbidden from reading past the High Watermark. This guarantees that any record returned to a consumer has already been durably replicated across all in-sync brokers.

### The Zero Data Loss Configuration Formula
To guarantee mathematically zero data loss during any single broker failure:
```properties
# Producer must wait for all in-sync replicas to acknowledge write
acks = all   # (or -1)

# Cluster must require at least 2 replicas to acknowledge before accepting write
min.insync.replicas = 2

# Topic replication factor must be at least 3
replication.factor = 3
```

---

## 4. Modern Tiered Storage (KIP-405)

Historically, keeping 1 year of data in Kafka required hundreds of terabytes of expensive local NVMe SSDs.

**Tiered Storage** decouples real-time low latency from historical retention:

```mermaid
flowchart LR
    LocalTier["Hot Local Tier (NVMe SSDs)<br/>- Retains last 24 to 48 hours<br/>- Serves sub-5ms real-time consumers"]
    
    LocalTier -->|Async Segment Upload| RemoteTier["Cold Remote Tier (S3 / GCS / Azure Blob / Ozone)<br/>- Retains 1 to 5 years<br/>- Cost: 90% cheaper than SSD<br/>- Serves backfill & batch consumers"]
```
