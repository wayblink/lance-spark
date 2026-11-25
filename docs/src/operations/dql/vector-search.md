# VECTOR_SEARCH

Run vector similarity search from Spark SQL using Lance namespace execution.

!!! warning "Spark Extension Required"
    `VECTOR_SEARCH` requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

!!! note "Namespace Tables Required"
    `VECTOR_SEARCH` resolves the `table` argument through a Spark catalog and executes through the Lance namespace `queryTable` API. Use a Lance namespace catalog table such as `lance.default.items`, not a raw Lance dataset path.

!!! note "Named Arguments"
    Named arguments require Spark 3.5 or later. On Spark 3.4, use the positional form.

!!! note "Replacing `nearest` Read Option"
    The previous DataFrame `nearest` read option has been removed. Use `VECTOR_SEARCH` for vector similarity search so execution goes through the Lance namespace API.

## Basic Usage

`VECTOR_SEARCH` returns the selected table columns plus `_distance`.

=== "SQL"
    ```sql
    SELECT id, title, _distance
    FROM VECTOR_SEARCH(
        table => 'lance.default.items',
        query_vector => array(0.12, 0.34, 0.56, 0.78),
        vector_column => 'embedding',
        num_results => 10,
        distance_type => 'l2',
        columns => array('id', 'title')
    )
    ORDER BY _distance;
    ```

## Column projection

`VECTOR_SEARCH` propagates the columns required by the outer Spark query to the Lance search scan. For example, the following query asks the native scan for `id` and the generated distance only; it does not need to materialize unrelated table columns:

```sql
SELECT id, _distance
FROM VECTOR_SEARCH(
    table => 'lance.default.items',
    query_vector => array(0.12, 0.34, 0.56, 0.78),
    num_results => 10
)
ORDER BY _distance;
```

`SELECT *` keeps the full table projection. A narrower outer projection is pushed into the native search: the scan reads only the payload columns the query uses, plus the fields ranking depends on (`_distance`, and the distributed tie-breaking `_rowid`). An explicit `columns => array(...)` decides the visible output schema and is the authoritative way to say which columns the result exposes, but it is not a request to materialize each of them in the native scan - a projected column is read only when the outer query needs it. A projection made only of metadata fields may conservatively keep the original native projection to preserve Lance's empty-column semantics. Nested struct reads keep the full struct layout internally so Arrow child-field positions stay aligned.

## Positional Form

Use positional arguments for simple calls and Spark 3.4 compatibility.

=== "SQL"
    ```sql
    SELECT *
    FROM VECTOR_SEARCH('lance.default.items', array(0.12, 0.34, 0.56), 5);
    ```

## Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `table` | String | Yes | Catalog table name to search. |
| `query_vector` | Array numeric literal | One of three | Query vector for a single-query search. |
| `queries` | Array string literal | One of three | Batch search: one comma-separated vector per element, for example `array('0.1,0.2', '0.3,0.4')`. |
| `query_table` | String | One of three | Batch search: a table holding one query vector per row. |
| `query_id_column` | String | No | Column of `query_table` to order the queries by, so `query_id` in the result is stable across runs. |
| `query_vector_column` | String | No | Vector column of `query_table`. Defaults to `vector`. |
| `vector_column` | String | No | Vector column name, or a path such as `payload.vec` for a nested column. Lance defaults to `vector` when omitted. |
| `num_results`, `limit`, or `k` | Integer | No | Number of results. Defaults to `10`. |
| `distance_type` | String | No | Distance metric: `l2` (`euclidean`), `cosine`, `dot` (`ip`, `inner_product`), or `hamming`. Aliases are normalized to the canonical name. |
| `columns` | Array string literal | No | Output table columns. `_distance` is always included. Use `array('*')` or omit this argument for all table columns. |
| `filter` | String | No | SQL filter expression evaluated by Lance. |
| `offset` | Integer | No | Number of results to skip. Lance Spark requests `num_results + offset` rows from Lance before applying the offset. Not supported with a batch search. |
| `version` | Long | No | Lance table version to search. |
| `nprobes`, `ef` | Integer | No | Vector index search tuning parameters. Raise `nprobes` to probe more IVF partitions, which is what recovers recall lost to an approximate search. |
| `index` | String | No | Name of the vector index to search. Distributed execution only; without it the first index on the column in manifest order is used. |
| `refine_factor` | Integer | No | Over-fetch `num_results * refine_factor` candidates from the index, then re-score them against the original vectors and keep the best `num_results`. Improves accuracy for quantized indexes such as IVF_PQ, at the cost of reading the vectors back. |
| `lower_bound`, `upper_bound` | Float | No | Distance bounds. |
| `bypass_vector_index`, `fast_search`, `prefilter`, `with_row_id` | Boolean | No | Lance query options. `with_row_id` adds `_rowid` to the output. `bypass_vector_index` and `fast_search` cannot both be true. |

## Output

The result includes the requested table columns and a nullable `_distance` float column. If `with_row_id => true`, or if `_rowid` is listed in `columns`, the result also includes Lance row ids.

A batch search returns `k` rows per query and puts a non-null `query_id` integer first. The id is
the query's position in the batch, zero-based: with `query_table` and `query_id_column` it follows
that column's order, otherwise the order the queries were given in.

## Batch search

One search can answer many queries. Name them inline, or point at a table that holds one vector per
row:

=== "Inline queries"
    ```sql
    SELECT query_id, id, _distance
    FROM VECTOR_SEARCH(
        table => 'lance.default.items',
        queries => array('0.12,0.34,0.56,0.78', '0.9,0.1,0.0,0.0'),
        vector_column => 'embedding',
        num_results => 10
    )
    ORDER BY query_id, _distance;
    ```

=== "Query table"
    ```sql
    SELECT query_id, id, _distance
    FROM VECTOR_SEARCH(
        table => 'lance.default.items',
        query_table => 'lance.default.item_queries',
        query_id_column => 'query_id',
        query_vector_column => 'embedding',
        vector_column => 'embedding',
        num_results => 10
    )
    ORDER BY query_id, _distance;
    ```

Why it is faster than a loop: each task opens its index segment once and answers every query in
that same scan, instead of paying the open-and-load cost per query. `num_results` is per query, so
`n` queries return at most `n * num_results` rows; the merge ranks rows inside their query, so one
query's results cannot crowd out another's.

A batch search requires `spark.sql.lance.search.distributed.enabled=true`: the namespace
`queryTable` response carries no per-query attribution, so the namespace path rejects a batch rather
than return rows that cannot be assigned to a query. `offset` and a batch are mutually exclusive.

## Choosing an index

`VECTOR_SEARCH(..., index => 'item_vec_cosine')` searches the named index. This only works with
distributed execution: the namespace `queryTable` API has no index field, so with
`spark.sql.lance.search.distributed.enabled=false` the option fails instead of being ignored.

Without `index`, the connector keeps its previous behaviour: the first index on the vector column in
manifest (creation) order, which is what lance-core itself searches. A named index also decides the
search column, so `vector_column` can be omitted; when both are given they must agree, compared as
field paths so an unquoted `my-vec` matches the quoted canonical form. An empty name is rejected, and
`bypass_vector_index => true` still uses the named index to resolve the column even though it does
not search that index.

An unknown name fails and lists the vector indexes the table has. A `distance_type` that contradicts
the index metric also fails - a named index is never silently replaced by a flat scan, because that
is how a query ends up measuring an index it did not ask for.

## Segment-local vector indexes

An `ivf_rq` index is stored as independent segments, each covering a contiguous run of fragments and
carrying its own IVF centroids and RaBitQ rotation. There is no shared model across segments, so
`nprobes` and `refine_factor` apply **per segment**. A segment with one IVF list probes that list
however small `nprobes` is. A segment with more lists probes at most `nprobes` of them, so values
below the partition count trade recall for speed; leaving `nprobes` unset uses Lance's default,
which probes every list for the segment sizes we measured. `refine_factor` over-fetches candidates
and re-scores them against the stored vectors, so it recovers the loss from coarse codes.

Use `refine_factor >= 10` for `num_bits >= 4` and `>= 30` for `1`-bit codes. With 8-bit codes and
`refine_factor = 10` the distributed result reproduces an exact `bypass_vector_index = true` scan row
for row; narrower candidate sets trade recall for less re-reading.

## Execution

By default, Spark plans `VECTOR_SEARCH` with one input partition and calls the Lance namespace
`queryTable` API. When `spark.sql.lance.search.distributed.enabled=true`, Spark instead opens the
dataset from its executors, searches vector-index segments and uncovered fragments in parallel,
and globally merges their candidates. The merge sorts by the distance each task reported, so each
task returns only its own top `num_results + offset` rows: a row outside a task's local top k
cannot enter the global top k. `nprobes` and `refine_factor` apply per task. A batch search ranks
inside each query before the top-k cut, so every query keeps its own `num_results` rows.

Distributed execution treats `nprobes` as an exact probe count. A namespace server can instead
forward it as a lower bound and let the search probe further, in which case the two paths reach
different recall for the same tuning. Re-check recall after enabling the flag; raise `nprobes`, or
use `bypass_vector_index => true` for an exact baseline, if the results need to be comparable.

Each distributed task reports the read-path metrics on its scan node — `numIops`, `numBytesRead`,
`numIndexComparisons` and the open/create/load timings — which is what lets a slow search be
attributed to storage or to the search itself. Namespace execution reports none of them. See
[Performance Tuning](../../performance.md#vector-search-metrics) for how to read the numbers.

A filtered distributed search must pass `prefilter=true`, which returns the true filtered top k.
Namespace execution defaults to `prefilter=false` and applies the filter after choosing the top k,
which usually returns fewer rows; a distributed plan cannot reproduce that, so omitting `prefilter`
with a `filter` fails during planning instead of silently changing the result. `lower_bound` and
`upper_bound` are not supported either. Disable distributed execution to run any of these through
the namespace.

### Deferring the payload read

A distributed search runs one task per index segment (plus one per unindexed fragment), and every
task reads its own local top-k **including the payload columns**. Only `num_results` rows per query
survive the global merge, so the fused plan reads the payload `units` times and ships all of it
through Spark's sort or exchange.

`spark.sql.lance.search.payload.deferral.mode` selects a second plan shape:

| Value | Behaviour |
| --- | --- |
| `off` (default) | Fused only: the native search reads payload inside every unit. |
| `auto` | Defer when one unit's estimated payload volume (`num_results x queries x row width`) reaches `spark.sql.lance.search.payload.deferral.min.bytes` (default 32 MiB). |
| `force` | Defer whenever the result carries a payload column, for comparing both shapes on the same data. |

With deferral, each unit reads only `_distance` and `_rowid` (plus `query_index` for a batch), Spark
ranks the candidates exactly as before, and the payload for the winners is then read by stable row
id at the **pinned snapshot** the search ran on. The second phase is chunked:
`spark.sql.lance.search.payload.deferral.chunk.rows` (default 1024) bounds the rows per fetch task,
and an internal 16 MiB budget lowers that further for wide rows, because `Dataset.takeRows` builds
one Arrow IPC response in memory per call. The plan shows `LanceFetchPayload` with the columns it
fetches; `EXPLAIN` therefore tells you which shape ran.

Two properties are worth knowing before turning it on. The fetch verifies every returned `_rowid`
against the requested one and fails the task on a mismatch, because lance-core silently drops a row
id that is not in the snapshot and a positional mapping would attach payload to the wrong row. And
retries are safe: a fetch task re-reads its chunk from the pinned snapshot, so a retried task
returns the same rows.

#### When it is not faster

- Small `num_results` with a narrow projection: the fused plan already reads very little, and the
  deferral adds a second dataset open plus a random-access pass.
- A single searchable unit: nothing is amplified, so the deferral only adds the second read.
- Payload that the query does not need but the TVF still declares: the deferred fetch reads the
  columns the TVF declares (`columns => ...`, or the whole table when the argument is omitted), not
  the subset a narrower outer `SELECT` picks. Pass `columns => array(...)` so the deferred arm reads
  only what it needs.
- A table whose rows are scattered over many fragments: `takeRows` has to touch every fragment that
  holds a winner, so random access can cost more than the sequential per-unit reads it replaces.

`auto` is a planning heuristic, not a measurement: it knows the declared projection and
`num_results` but not how many units the dataset will produce. Keep `off` unless a workload is wide
and multi-segment, and re-check recall and latency after enabling it.

## Validation


The Docker integration suite covers `VECTOR_SEARCH` against the directory namespace and a REST namespace backed by a directory namespace. The `Spark Search Docker` GitHub Actions workflow runs both backends for pull requests.
