# 🏛️ Lecture 01: Open Table Formats: Apache Iceberg vs. Delta Lake
### *The Mechanics of Snapshot Isolation, Metadata Trees & ACID Transactions*

---

## 1. Why Hive Tables Failed on Cloud Object Storage

For 15 years, Big Data relied on the **Hive Metastore Table Format**. A Hive table was defined as:
$$\text{Hive Table} \equiv \text{A Directory of Files on Disk}$$

When organizations migrated from bare-metal HDFS to Cloud Object Storage (Amazon S3, Azure ADLS, Google Cloud Storage), this directory-based model collapsed:

```mermaid
flowchart TD
    subgraph HiveFailure["❌ Why Directory-Based Hive Tables Break on S3"]
        direction TB
        F1["1. S3 LIST Overhead: S3 paginates 1,000 files/request.<br/>Listing a table with 500,000 files takes 12 MINUTES before a query even starts!"]
        F2["2. Non-Atomic Renames: S3 has NO rename API.<br/>Renaming a folder requires copying every byte, then deleting.<br/>A crash leaves partial, permanently corrupted data."]
        F3["3. Dirty Reads: If Job B overwrites a directory while Query A is running,<br/>Query A reads half-old and half-new files, producing corrupted financial metrics."]
    end
```

---

## 2. Apache Iceberg: The Metadata Tree Architecture

Apache Iceberg discarded the directory model entirely. In Iceberg:
$$\text{Iceberg Table} \equiv \mathbf{An\ Immutable\ Tree\ of\ Metadata\ Files}$$

```mermaid
flowchart TB
    subgraph CatalogLevel["1. Iceberg Catalog (Glue / Nessie / JDBC)"]
        Catalog["Catalog Pointer -> Points atomically to 's3://bucket/metadata/v3.metadata.json'"]
    end

    subgraph MetadataLevel["2. Table Metadata (v3.metadata.json)"]
        direction TB
        MetaJSON["Table Metadata (v3.metadata.json)<br/>- Table Schema (Field IDs, Types)<br/>- Partition Spec (Month, Identity)<br/>- Current Snapshot ID: 1002"]
        SnapLog["Snapshot Log: [Snap 1001, Snap 1002]"]
        MetaJSON --> SnapLog
    end

    subgraph ManifestListLevel["3. Manifest List (snap-1002.avro)"]
        SnapAvro["Manifest List File (snap-1002.avro)<br/>Records all active manifest files + partition summary bounds"]
    end

    subgraph ManifestFileLevel["4. Manifest Files (*.avro)"]
        ManA["Manifest A (manifest-a.avro)<br/>- Data file paths<br/>- File Status: ADDED / EXISTING<br/>- Min / Max bounds per column"]
        ManB["Manifest B (manifest-b.avro)<br/>- Data file paths<br/>- Min / Max bounds per column"]
    end

    subgraph DataLevel["5. Data Files (*.parquet)"]
        D1["file_01.parquet"]
        D2["file_02.parquet"]
        D3["file_03.parquet"]
    end

    Catalog -->|Atomic Pointer| MetaJSON
    SnapLog -->|Points to Current Snapshot| SnapAvro
    SnapAvro --> ManA & ManB
    ManA --> D1 & D2
    ManB --> D3
```

### The 4 Layers of Iceberg Metadata
1. **The Catalog**: Stores a single string pointing to the current `metadata.json` file. Updating a table is a **Compare-and-Swap (CAS)** operation on this pointer.
2. **Table Metadata File (`vX.metadata.json`)**: Tracks schema definitions, partition specifications, and historical snapshot IDs.
3. **Manifest List (`snap-ID.avro`)**: Represents a specific snapshot. It lists all active manifest files and includes **partition summaries**. 
   - *Optimization*: If a query filters for `date = '2026-10'`, Iceberg inspects the manifest list and prunes entire manifest files without opening them!
4. **Manifest File (`*.avro`)**: Stores individual data file paths, row counts, and **column-level upper/lower bounds** (e.g., `min(user_id) = 1`, `max(user_id) = 50000`).
   - *Optimization*: If a query asks for `user_id = 99999`, Iceberg uses the manifest bounds to skip reading `file_01.parquet` entirely.

---

## 3. Delta Lake: The Transaction Log Protocol

Delta Lake implements ACID transactions using an ordered **JSON commit log** and periodic **Checkpoint Parquet files**.

```mermaid
flowchart TB
    subgraph DeltaLog["_delta_log/ Directory"]
        direction TB
        C0["000000.json (Initial Commit)"]
        C1["000001.json (Append 5 Parquet Files)"]
        C2["000002.json (Delete 2 Parquet Files)"]
        Checkpoint["000010.checkpoint.parquet<br/>(Squashed state of commits 0 to 10)"]
        C11["000011.json"]

        C0 --> C1 --> C2 --> Checkpoint --> C11
    end

    subgraph DataFiles["Parquet Storage"]
        P1["part-0000.parquet (Active)"]
        P2["part-0001.parquet (Tombstoned by commit 2)"]
    end

    C1 -. References .-> P1 & P2
    C2 -. Removes .-> P2
```

### The Delta Commit Mechanics
1. **Optimistic Concurrency Control (OCC)**:
   - When a writer begins, it notes the latest commit version (e.g., Version `10`).
   - It writes new Parquet data files to storage.
   - It attempts to write `000011.json`.
   - If another writer committed Version `11` first, the writer checks whether the concurrent transaction mutated the same partitions. If not, it automatically retries and commits Version `12`.
2. **Checkpoints**:
   - Replaying hundreds of JSON files to find the current state would become slow.
   - Every **10 commits**, Delta automatically compacts the entire log state into a single **Checkpoint Parquet file** (`000010.checkpoint.parquet`). Query engines only read the latest checkpoint plus subsequent JSON files.

---

## 4. ACID Guarantees & Time-Travel Queries

Because every snapshot is immutable, both Iceberg and Delta Lake provide native **Snapshot Isolation** and **Time-Travel**.

```sql
-- Query the exact state of the table as of 2 hours ago
SELECT * FROM prod.fact_orders 
TIMESTAMP AS OF '2026-10-04 10:00:00';

-- Query a specific snapshot ID after a known ETL job finished
SELECT * FROM prod.fact_orders 
VERSION AS OF 1002;
```

### How Time-Travel Enables Instant Incident Rollbacks
If a rogue ETL pipeline writes bad data at 11:00 AM:
```sql
-- Instantly revert production table to the snapshot before the incident!
-- Execution time: < 1 second (atomic metadata pointer swap)
CALL prod.system.rollback_to_snapshot('prod.fact_orders', 1001);
```
No physical files are moved or deleted. The table pointer immediately points back to snapshot `1001`, instantly restoring clean data for downstream consumers.
