# Performance Tuning

This guide covers performance tuning for Lance Spark operations in large-scale ETL and batch processing scenarios.

## Understanding Lance's Default Optimization

Lance is **optimized by default for random access patterns** - fast point lookups, vector searches, and selective column reads.
These defaults work well for ML/AI workloads where you frequently access individual records or small batches.

For **large-scale batch ETL and scan-heavy OLAP operations** (writing millions of rows, full table scans, bulk exports),
you can tune Lance's environment variables and Spark options to better utilize available resources.

## General Recommendations

For optimal performance with Lance, we recommend:

1. **Use fixed-size lists for vector columns** - Store embedding vectors as `ARRAY<FLOAT>` with a fixed dimension. This enables efficient SIMD operations and better compression. See [Vector Columns](operations/ddl/create-table.md#vector-columns).

2. **Use blob encoding for multimodal data** - Store large binary data (images, audio, video) using Lance's unique blob encoding to enable efficient access. See [Blob Columns](operations/ddl/create-table.md#blob-columns).

3. **Use large varchar for large text** - When storing very large string values for use cases like full text search, use the large varchar type to avoid out of memory issues. See [Large String Columns](operations/ddl/create-table.md#large-string-columns).

## Write Performance

### Upload Concurrency

Set via environment variable `LANCE_UPLOAD_CONCURRENCY` (default: 10).

Controls the number of concurrent multipart upload streams to S3. 
Increasing this to match your CPU core count can improve throughput.

```bash
export LANCE_UPLOAD_CONCURRENCY=32
```

### Upload Part Size

Set via environment variable `LANCE_INITIAL_UPLOAD_SIZE` (default: 5MB).

Controls the initial part size for S3 multipart uploads. 
Larger part sizes reduce the number of API calls and can improve throughput for large writes. 
However, larger part sizes use more memory and may increase latency for small writes. 
Use the default for interactive workloads.

!!!note
      Lance automatically increments the multipart upload size by 5MB every 100 uploads,
      so large file writes progressively use increasingly large upload parts.
      There is no configuration for a fixed upload size.

```bash
export LANCE_INITIAL_UPLOAD_SIZE=33554432  # 32MB
```

### Queued Write Buffer (Experimental)

Set via Spark write option `use_queued_write_buffer` (default: false).

Enables a buffered write mode that improves throughput for large batch writes.
When enabled, Lance uses a queue-based buffer instead of the default semaphore-based buffer
that batches data more efficiently before writing to storage,
avoiding small lock waiting operations between Spark producer and Lance writer.

!!!note
      This feature is currently in experimental mode.
      It will be set to true by default after the community considers it mature. 

```python
df.write \
    .format("lance") \
    .option("use_queued_write_buffer", "true") \
    .mode("append") \
    .saveAsTable("my_table")
```

### Max Batch Bytes

Set via Spark write option `max_batch_bytes` (default: 268435456, i.e. 256MB).

Controls the maximum in-memory size of each Arrow batch before it is flushed.
Batches are flushed when either the row count reaches `batch_size` or the allocated memory
reaches `max_batch_bytes`, whichever comes first.

This prevents OOM when writing tables with very large rows — wide schemas, large binary/string
columns, or high-dimensional vector embeddings — where even a modest number of rows can exhaust
memory before the row-count threshold is reached.

Small-row workloads are unaffected because the row-count limit is hit first.

!!!note
      When using the queued write buffer, total in-flight Arrow memory can be roughly
      `queue_depth * max_batch_bytes`. You may need to tune `queue_depth` and `max_batch_bytes`
      together to stay within memory limits.

```python
df.write \
    .format("lance") \
    .option("max_batch_bytes", "134217728") \
    .mode("append") \
    .saveAsTable("my_table")  # 128MB per batch
```

### Max Rows Per File

Set via Spark write option `max_row_per_file` (default: 1,000,000).

Controls the maximum number of rows per Lance fragment file.
There is no specific recommended value, but be aware the default is 1 million rows.
If you store many multimodal data columns (images, audio, embeddings)
without using [Lance blob encoding](operations/dml/insert-into.md#writing-blob-data),
or store a lot of long text columns, the file size might become very large.
From Lance's perspective, having very large files does not impact your read performance.
But you may want to reduce this value depending on the limits in your choice of object storage.

```python
df.write \
    .format("lance") \
    .option("max_row_per_file", "500000") \
    .mode("append") \
    .saveAsTable("my_table")
```

### Large Var Types

Set via Spark write option `use_large_var_types` (default: false).

Switches all string and binary columns to use 64-bit offset Arrow vectors
(`LargeVarCharVector` / `LargeVarBinaryVector`) instead of the default 32-bit offset vectors.
This removes the 2GB-per-batch data buffer limit that can cause `OversizedAllocationException`
when writing rows with very large string or binary values.

Use this when your rows contain large values (hundreds of KB or more per row) and you
hit the 2GB overflow error. There is no meaningful performance overhead -- the only
difference is 8 bytes per row for the offset buffer instead of 4.

```python
df.write \
    .format("lance") \
    .option("use_large_var_types", "true") \
    .mode("append") \
    .saveAsTable("my_table")
```

!!!note
    For per-column control at table creation time, see the
    [`arrow.large_var_char` table property](operations/ddl/create-table.md#large-string-columns).

## Read Performance

### I/O Threads

Set via environment variable `LANCE_IO_THREADS` (default: 64).

Controls the number of I/O threads used for parallel reads from storage. 
For large scans, increasing this to match your CPU core count enables more concurrent S3 requests.

```bash
export LANCE_IO_THREADS=128
```

### Custom Read Metrics

Lance Spark reports per-task custom metrics on the Spark UI Scan node, viewable in the SQL tab's
physical plan. These are useful for diagnosing read-path performance — pruning effectiveness, JNI
overhead, and where time is being spent.

Naming conventions:

- Counters use the `num*` prefix.
- Durations use the `*TimeNs` suffix and are displayed in the UI as formatted strings
  (e.g. `1.2 s`, `350 ms`, `47 us`) rather than raw nanoseconds.

| Metric | Type | Description |
|---|---|---|
| `numFragmentsScanned` | counter | Lance fragments actually opened by this task. Compare against the table fragment count to verify pruning is working. |
| `numBatchesLoaded` | counter | Arrow batches returned from the JNI scanner. |
| `numRowsScanned` | counter | Rows read from storage before filter evaluation. Pair with Spark's built-in `numOutputRows` to compute filter selectivity (`numOutputRows / numRowsScanned`). |
| `numIops` | counter | Number of I/O operations performed by the scanner. |
| `numRequests` | counter | Number of requests made to the storage layer. |
| `numBytesRead` | counter | Total bytes read from storage by the scanner. |
| `numIndicesLoaded` | counter | Number of top-level indices loaded. |
| `numPartsLoaded` | counter | Number of index partitions loaded. |
| `numIndexComparisons` | counter | Number of index comparisons performed during vector search. |
| `numIndexCacheHits` / `numIndexCacheMisses` | counters | Native index-cache lookup hits and misses reported by this scanner. These count cache lookups, not resident files or bytes. |
| `datasetOpenTimeNs` | duration | Time spent in `Dataset.open(...)` — manifest load, namespace lookup, credential fetch. High values indicate catalog or metadata cache misses. |
| `scannerCreateTimeNs` | duration | Time spent in `fragment.newScan(...)` — scan planning, predicate compilation, index lookup setup. |
| `batchLoadTimeNs` | duration | Wall-clock time to produce one batch and hand it to Spark: JNI crossing, IO, Arrow IPC deserialization, and building the Spark column vectors. Both the table read path and the distributed search path stop the timer at that same point, so the two are comparable. Divide by `numBatchesLoaded` to get per-batch cost. |

How to read these together:

- **Pushdown verification**: a low `numRowsScanned / numOutputRows` ratio means filters are being
  pushed down effectively. A 1:1 ratio means every row is being scanned and filtered in Spark —
  check whether your predicate is supported for pushdown.
- **Batch production cost**: `batchLoadTimeNs / numBatchesLoaded` is the average time producing and
  importing a returned batch. It includes native computation, I/O and Arrow import; it does not
  isolate JNI overhead. Compare it with the I/O and index-work counters before tuning batch size.
- **Catalog overhead**: `datasetOpenTimeNs` accumulates per fragment opened. If many fragments are
  opened per task, this can dominate; metadata cache size and namespace caching matter most here.

### Vector search metrics

`VECTOR_SEARCH` planned as Spark tasks (`spark.sql.lance.search.distributed.enabled=true`) reports
the same metrics on its scan node, one task per index segment or uncovered fragment. Read them a
little differently than a table scan:

- `batchLoadTimeNs` is the time spent producing the search results: probing the index, scanning
  candidate codes, computing distances, fetching the matching rows, and wrapping the returned Arrow
  vectors as Spark columns (the same boundary the table read path uses). `scannerCreateTimeNs`
  covers `newScan` plus starting the Arrow stream, and `datasetOpenTimeNs` covers opening the
  dataset on the worker.
- `numIops` / `numRequests` / `numBytesRead` count what the task pulled from storage. These are what
  separate "the store got slower" from "the search got slower": if `batchLoadTimeNs` grows while the
  counters stay flat, the time went to compute; if they grow together, it went to IO.
- `numIndexComparisons` / `numPartsLoaded` are non-zero only for tasks that searched an index
  segment; a run that fell back to flat KNN reports zeros there. `numIndicesLoaded` counts whole
  top-level indices opened by the scanner and usually stays zero for a per-segment search.
- `numIndexCacheHits` / `numIndexCacheMisses` are this scanner's native index-cache lookup counts.
  A hit does not prove that a whole index file, all IVF lists, or the full table's indexes reside in
  memory. Pair the counts with bytes read, parts loaded, executor placement and the configured
  native cache budget; the connector does not report index-residency coverage. A `0 / 0` pair is
  not a 100% miss rate: it means the cache was not consulted on this path (cache disabled, or the
  index was reached without the cache layer) or the scanner reported no statistics at all. The
  reader treats missing stats as diagnostics only and never fails a task over them, so check whether
  the other counters are populated before reading a zero pair as a measurement.
- `numRowsScanned` comes from Lance's own per-scan counter: rows read from storage, including the
  fetches that assemble the top-k rows. It measures work done rather than rows matched, and is the
  first thing to compare between two runs — if both read the same rows, a wall-clock difference is
  not a difference in the work. Lance's `fragments_scanned` counter is deliberately not mapped to
  `numFragmentsScanned`: it counts physical fragment scans (2 for a task that scans one fragment),
  so it would not mean what that scan-node column says.

The distributed search scan also publishes two **driver-only** durations on its SQL scan node:

| Metric | Description |
|---|---|
| `driverPlanningTimeNs` | One partition-planning attempt, including dataset open/close, index discovery and unit creation. |
| `driverIndexDiscoveryTimeNs` | The portion of planning spent resolving the column and discovering/selecting the vector index, including statistics fallback when needed. |

These are snapshots from planning, not task counters: repeated reporting or reading the cached
partition plan does not add them again, and reading metrics never starts dataset I/O. Before planning,
zero means not measured. The discovery duration is included in planning; do not add them together.
Plans, the driver planning log and the executor's per-task line (which carries `Lance distributed
search task: scanId=...`) all share the same stable `scanId`, so a slow or failed task can be
attributed to the scan that planned it.
Use Spark's existing Sort, Window, Exchange and stage metrics for global merging rather than adding
another overlapping merge timer. Summed task durations describe work across workers, not query
wall-clock time; `batchLoadTimeNs` is not a JNI-only timer. Metrics collection does not change the
search's candidates, ordering or cache policy.

Namespace-executed search (the default) has no equivalent. It returns results through `queryTable`,
whose response carries Arrow data only, so its scan node shows no Lance metrics at all; attribute
search performance with the distributed path enabled.

The connector asks Lance to collect these counters (`ScanOptions.collectStats`). Without that flag
the scanner reports zeros for every IO counter. The counters are published when the scan runs dry,
so a task that is cancelled or stopped early — a `LIMIT` above the search that lets Spark stop
reading — can show zeros even though it did the work.

### Dataset lease and planning reuse

A distributed search opens the dataset in every task and reads the fragment list and the vector
index metadata in every planning pass. Both are version-fixed, so a later task or query on the same
version reuses them. Three counters make the effect visible:

- `numDatasetOpens` is the number of datasets the task had to open. Without the lease it equals the
  number of search tasks; with it, one task per executor opens and the rest report
  `numDatasetLeaseHits`.
- `datasetOpenTimeNs` is the cost that disappears with those opens.
- `driverIndexDiscoveryTimeNs` drops to zero on a query whose version was already planned on this
  driver.

A local run of an eight-insert table (32 fragments, 32 search tasks, four local slots, no
cross-process IO) shows the shape of the saving:

| | opens | lease hits | `datasetOpenTimeNs`, summed over tasks | `driverIndexDiscoveryTimeNs` |
| --- | --- | --- | --- | --- |
| one query, lease off | 32 | 0 | 75.1 ms | n/a (five-fragment case: 7.3 ms) |
| one query, lease on | 1 | 31 | 10.2 ms | 5.6 ms cold, 0 ms on the second query |

These are per-JVM numbers on a local dataset: they show that the open is a real per-task cost and
that the reuse removes it, not what a 10M-row table on a cluster will save. Reward the numbers on
the cluster, where an open also costs an object-store metadata round trip and, with executor
credential refresh, a `describeTable` call. The saving is bounded by design: an entry older than
`spark.sql.lance.search.dataset.lease.max.age.ms` is reopened rather than reused, which is what
keeps a reused namespace client from serving past its credentials' lifetime.

What the lease deliberately does not do: it never reuses across versions, credentials or namespace
roots, and it does not lease "latest" — only a reference that pins a concrete version, which is what
the planner hands each task. A read whose options say "latest" (the deferred payload fetch, as it
runs today) opens per consumer exactly as before, so the mechanism can be turned off without
changing a single row of the answer.

### Custom Write Metrics

The write path reports two per-task metrics.

| Metric | SQL tab | Stage metric | Description |
|---|---|---|---|
| `recordsWritten` | yes | `outputRecords` | Rows handed to this task's writer. |
| `bytesWritten` | no | `outputBytes` | Total size of the Lance data files this task produced. |

Spark special-cases both names: `execution.metric.CustomMetrics.updateMetrics` matches exactly
`bytesWritten` and `recordsWritten` and forwards them to the task's output metrics, which is what
populates stage-level `outputBytes` / `outputRecords` in the Stages UI and the history server REST
API. That happens whether or not the connector advertises them as SQL metrics.

`bytesWritten` is not advertised as a SQL metric. SQL metrics come from
`DataWriter.currentMetricsValues()`, whose last poll is just before `commit()`, and Lance fragments
only complete inside `commit()` unless the write is sharded. The SQL tab would therefore show 0, so
the writer publishes the byte total straight to output metrics at the end of `commit()` instead.

A failed task attempt clears its own output metrics in `abort()`. Spark folds a task's metrics into
the stage totals whatever its end reason, so without that clear a retried write would report the
failed attempt's partial rows on top of the retry's full count.

## Caching

Lance Spark uses a multi-level caching strategy to minimize redundant I/O and improve query performance.

!!!note
    Caches are isolated per Spark catalog. For multi-tenant deployments, configure one Lance catalog per tenant to provide complete cache and credential isolation. If you configure multiple catalogs, total memory usage is multiplied by the number of catalogs. For example, with default settings (6GB index + 1GB metadata) and 3 catalogs, the maximum cache memory is 21GB (3 × 7GB).

### How Caching Works

Lance Spark implements two levels of caching:

1. **Session Cache** - Contains index and metadata caches:

    - **Index Cache**: Caches opened vector indices, fragment reuse indices, and index metadata
    - **Metadata Cache**: Caches manifests, transactions, deletion files, row ID indices, and file metadata

2. **Dataset Cache** - Caches opened datasets by `(catalog, URI, version)` key. Since a dataset at a specific version is immutable, this ensures:

    - Each dataset is opened only once per worker
    - All workers read the same version for snapshot isolation
    - Fragments are pre-loaded and cached per dataset

### Cache Backend

The index and metadata cache backend can be selected independently for each Spark catalog. For
example:

```bash
--conf 'spark.sql.catalog.lance.index_cache_backend=moka://?capacity=8589934592' \
--conf 'spark.sql.catalog.lance.metadata_cache_backend=moka://?capacity=2147483648'
```

See [Cache Backends](config.md#cache-backends) for backend registration, switching, and precedence.

### Index Cache Size

Set via environment variable `LANCE_INDEX_CACHE_SIZE` (default: 6GB from Lance native).

Controls the size of the index cache in bytes.
The index cache stores vector indices which can be large but provide significant speedup for vector search queries.
Increase this if you frequently query tables with vector indices.

```bash
export LANCE_INDEX_CACHE_SIZE=8589934592  # 8GB
```

### Metadata Cache Size

Set via environment variable `LANCE_METADATA_CACHE_SIZE` (default: 1GB from Lance native).

Controls the size of the metadata cache in bytes.
The metadata cache stores manifests, file metadata, and other dataset metadata.
Each column's metadata can be around 40MB, so increase this if your tables have many columns.

```bash
export LANCE_METADATA_CACHE_SIZE=2147483648  # 2GB
```
