/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.search;

import org.lance.Dataset;
import org.lance.index.DistanceType;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.lance.ipc.ScanStats;
import org.lance.spark.LanceConstant;
import org.lance.spark.internal.ExecutorNamespace;
import org.lance.spark.read.metric.LanceReadMetricsTracker;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Worker-side partition reader for distributed VECTOR_SEARCH. Opens the Lance dataset locally and
 * runs one of:
 *
 * <ul>
 *   <li>indexed unit ({@code indexSegments} non-empty) → {@code ScanOptions.indexSegments(...)}
 *   <li>fallback unit ({@code fragmentIds} non-empty) → {@code ScanOptions.fragmentIds(...)}
 * </ul>
 *
 * and iterates Arrow batches into Spark {@link ColumnarBatch}.
 *
 * <p>A batch search runs one native scan per chunk of its queries (see {@link
 * LanceBatchChunkPlan}), all on the one dataset this task opened. Nothing is renumbered: lance-core
 * reports a {@code query_index} relative to the scan it ran, so the reader adds the chunk's base to
 * it, and {@code query_id} stays the query's position in the input sequence.
 */
public class LanceDistributedSearchColumnarPartitionReader
    implements PartitionReader<ColumnarBatch> {
  /**
   * Lance's free-form scan counter for physical row reads. It is not part of {@link ScanStats}'
   * fixed fields, it is an internal name rather than a documented API - so it is read only when
   * present, and an integration test fails if an upstream rename drops it.
   *
   * <p>It counts rows read from storage including the fetches that assemble the top-k rows, so it
   * is a measure of work done, not of rows matched. Lance's {@code fragments_scanned} counter is
   * deliberately not mapped to {@code numFragmentsScanned}: it counts physical fragment scans and
   * reports 2 for a task that scans one fragment.
   */
  private static final String STAT_ROWS_SCANNED = "rows_scanned";

  /** The column lance-core appends to a batch result, naming the query each row belongs to. */
  static final String QUERY_INDEX_COLUMN = "query_index";

  private static final Logger LOG =
      LoggerFactory.getLogger(LanceDistributedSearchColumnarPartitionReader.class);

  private final LanceDistributedSearchInputPartition partition;
  private final LanceBatchChunkPlan chunkPlan;
  private final LanceReadMetricsTracker metricsTracker = new LanceReadMetricsTracker();
  private final LanceBatchChunkMetrics batchMetrics = new LanceBatchChunkMetrics();
  private final CustomTaskMetric[] taskMetrics;

  private ExecutorNamespace executorNamespace;
  private Dataset dataset;
  private LanceScanner scanner;
  private ArrowReader reader;
  private ColumnarBatch currentBatch;
  private int chunkIndex = -1;
  private int chunkBase;
  private boolean chunkStatsCollected;
  private boolean finished;

  public LanceDistributedSearchColumnarPartitionReader(
      LanceDistributedSearchInputPartition partition) {
    this.partition = partition;
    this.chunkPlan = partition.getBatchChunkPlan();
    // Both trackers hand out one array of live views, so the concatenation happens once per reader
    // instead of on every currentMetricsValues() call.
    CustomTaskMetric[] read = metricsTracker.currentMetricsValues();
    CustomTaskMetric[] chunks = batchMetrics.currentMetricsValues();
    this.taskMetrics = Arrays.copyOf(read, read.length + chunks.length);
    System.arraycopy(chunks, 0, this.taskMetrics, read.length, chunks.length);
  }

  @Override
  public boolean next() throws IOException {
    if (finished) {
      return false;
    }
    while (true) {
      if (reader == null && !openNextChunkScan()) {
        finished = true;
        return false;
      }
      long batchStart = System.nanoTime();
      boolean hasNext = reader.loadNextBatch();
      if (hasNext) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        // Chunk-local indices have to become global before the columns exist: the plan ranks by
        // query_index, so two chunks of the same query would otherwise look like different ones.
        if (chunkBase != 0) {
          rebaseQueryIndex(root);
        }
        currentBatch =
            LanceSearchColumnarPartitionReader.toColumnarBatch(root, partition.getSchema());
        // Stopped once the Spark columns exist, not right after loadNextBatch: the read path
        // measures all of LanceFragmentColumnarBatchScanner.loadNextBatch, which includes
        // building the column vectors. Wrapping the Arrow vectors is cheap but not free, and
        // stopping earlier would make this metric systematically smaller than the read path's
        // while the docs describe both as the batch-production time.
        metricsTracker.addBatchLoadTimeNs(System.nanoTime() - batchStart);
        metricsTracker.addNumBatchesLoaded(1);
        return true;
      }
      // This chunk is dry. Spark samples the reader's metrics before it calls close()
      // (DataSourceRDD's task-completion listener), so each chunk's counters have to be published
      // as soon as its scan ends, not only in close().
      captureScanStats();
      closeChunkScan();
    }
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    // Fallback for a reader closed before its scans ran dry (cancellation, early stop): the
    // completion listener has already sampled whatever was published by then.
    captureScanStats();
    Throwable first = null;
    if (currentBatch != null) {
      try {
        currentBatch.close();
      } catch (Throwable t) {
        first = t;
      }
    }
    first = closeChunkScan(first);
    first = closeQuietly(dataset, first);
    first = closeQuietly(executorNamespace, first);
    if (first != null) {
      throw new IOException("Failed to close LanceDistributedSearchColumnarPartitionReader", first);
    }
  }

  /**
   * Publishes the current chunk's scanner counters. Scan stats are cumulative for one scan, so they
   * are collected exactly once per chunk and summed across chunks.
   */
  private void captureScanStats() {
    if (scanner == null || chunkStatsCollected) {
      return;
    }
    chunkStatsCollected = true;
    try {
      Optional<ScanStats> stats = scanner.getStats();
      if (stats.isEmpty()) {
        return;
      }
      metricsTracker.addScanStats(stats);
      Map<String, Long> counts = stats.get().getAllCounts();
      if (counts != null) {
        metricsTracker.addNumRowsScanned(counts.getOrDefault(STAT_ROWS_SCANNED, 0L));
      }
    } catch (Throwable ignored) {
      // best effort: the counters are diagnostics, not correctness
    }
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    return taskMetrics;
  }

  private static Throwable closeQuietly(AutoCloseable closeable, Throwable carried) {
    if (closeable == null) {
      return carried;
    }
    try {
      closeable.close();
      return carried;
    } catch (Throwable t) {
      return carried == null ? t : carried;
    }
  }

  private Throwable closeChunkScan(Throwable carried) {
    Throwable first = closeQuietly(reader, carried);
    reader = null;
    first = closeQuietly(scanner, first);
    scanner = null;
    chunkStatsCollected = false;
    return first;
  }

  private void closeChunkScan() {
    Throwable failure = closeChunkScan(null);
    if (failure != null) {
      LOG.warn(
          "Lance distributed search could not close a chunk scan for scanId={}: {}",
          partition.getScanId(),
          failure.getMessage());
    }
  }

  /**
   * Opens the dataset once per task, then one scanner per chunk. Chunking here rather than in the
   * plan is deliberate: more Spark tasks would open the dataset and its index once per chunk and
   * would run concurrently, so the peak working set would not shrink.
   */
  private boolean openNextChunkScan() throws IOException {
    int next = chunkIndex + 1;
    if (next >= chunkPlan.chunks()) {
      return false;
    }
    chunkIndex = next;
    chunkBase = chunkPlan.baseOf(next);
    if (dataset == null) {
      openDataset();
    }
    LanceSearchQuery chunkQuery = queriesOfChunk();
    ScanOptions opts = buildScanOptions(partition, chunkQuery);
    long scanStart = System.nanoTime();
    try {
      scanner = dataset.newScan(opts);
      reader = scanner.scanBatches();
      metricsTracker.addScannerCreateTimeNs(System.nanoTime() - scanStart);
    } catch (Exception e) {
      throw new IOException(
          "Failed to open distributed search scan for scanId="
              + partition.getScanId()
              + ", partition: "
              + describe(partition)
              + ", chunk: "
              + chunkIndex
              + '/'
              + chunkPlan.chunks(),
          e);
    }
    chunkStatsCollected = false;
    if (partition.getQuery().isBatch()) {
      int queries = chunkPlan.queriesInChunk(next);
      batchMetrics.addChunk(queries, chunkPlan.estimatedBytesOfChunk(next));
      LOG.info(
          "Lance distributed search task chunk: scanId={}, unit={}, chunk={}/{}, queries=[{}, {}), "
              + "estimatedBytes={}",
          partition.getScanId(),
          describe(partition),
          next,
          chunkPlan.chunks(),
          chunkBase,
          chunkBase + queries,
          chunkPlan.estimatedBytesOfChunk(next));
    }
    return true;
  }

  private void openDataset() throws IOException {
    LanceSearchQuery query = partition.getQuery();
    executorNamespace =
        ExecutorNamespace.acquire(
            query.getReadOptions(), query.getNamespaceImpl(), query.getNamespaceProperties());
    long openStart = System.nanoTime();
    dataset =
        Utils.openDatasetBuilder(query.getReadOptions())
            .initialStorageOptions(query.getInitialStorageOptions())
            .build();
    metricsTracker.addDatasetOpenTimeNs(System.nanoTime() - openStart);
    // One line per task, so an executor-side log can be matched to the driver's planning line for
    // the same scan (both carry scanId) instead of guessing by timing or by fragment numbers.
    LOG.info(
        "Lance distributed search task: scanId={}, unit={}, chunks={}, datasetOpenTimeNs={}",
        partition.getScanId(),
        describe(partition),
        chunkPlan.chunks(),
        metricsTracker.getDatasetOpenTimeNs());
  }

  /**
   * The queries this chunk submits. A single-vector search, and a batch that needs no cut, are
   * submitted unchanged, so an unchunked batch is bit-for-bit the query it always was.
   */
  private LanceSearchQuery queriesOfChunk() {
    LanceSearchQuery base = partition.getQuery();
    if (!base.isBatch() || chunkPlan.isSingleChunk()) {
      return base;
    }
    return base.subBatch(chunkBase, chunkPlan.queriesInChunk(chunkIndex));
  }

  /**
   * Adds the chunk's base to every row's {@code query_index}, turning the scan-local index into the
   * query's position in the batch.
   *
   * <p>Mutating the Arrow vector rather than wrapping it keeps the per-row work an int add instead
   * of a virtual call per cell, and the batch is converted to Spark columns right after.
   */
  private void rebaseQueryIndex(VectorSchemaRoot root) {
    IntVector vector = queryIndexOf(root);
    int rows = root.getRowCount();
    for (int row = 0; row < rows; row++) {
      vector.set(row, vector.get(row) + chunkBase);
    }
  }

  /**
   * The attribution column of a batch result.
   *
   * <p>Normally there is exactly one, but a table may carry a column of its own called {@code
   * query_index} and the caller may select it, which makes the name ambiguous. lance-core puts its
   * attribution column first, so that is the answer in that case - and a warning says which
   * assumption was made, rather than shifting a payload column by the chunk base.
   */
  static IntVector queryIndexOf(VectorSchemaRoot root) {
    List<IntVector> candidates = new ArrayList<>();
    boolean named = false;
    for (FieldVector vector : root.getFieldVectors()) {
      if (!QUERY_INDEX_COLUMN.equals(vector.getField().getName())) {
        continue;
      }
      named = true;
      if (vector instanceof IntVector) {
        candidates.add((IntVector) vector);
      }
    }
    if (candidates.isEmpty()) {
      throw new IllegalStateException(
          named
              ? "The batch search result carries a query_index column that is not an integer, so "
                  + "the rows cannot be attributed to their queries"
              : "The batch search result carries no query_index column, so the rows cannot be "
                  + "attributed to their queries");
    }
    if (candidates.size() == 1) {
      return candidates.get(0);
    }
    FieldVector first = root.getFieldVectors().get(0);
    if (first == candidates.get(0)) {
      LOG.warn(
          "The batch search result has {} integer columns named {}, so a dataset column shares the "
              + "internal name; the first column is taken as the query attribution, which is the "
              + "layout lance-core writes",
          candidates.size(),
          QUERY_INDEX_COLUMN);
      return candidates.get(0);
    }
    throw new IllegalStateException(
        "The batch search result has "
            + candidates.size()
            + " integer columns named "
            + QUERY_INDEX_COLUMN
            + " and the first column is not one of them, so chunks cannot be attributed to their "
            + "queries; rename the dataset column or select it under another alias");
  }

  static ScanOptions buildScanOptions(LanceDistributedSearchInputPartition p) {
    return buildScanOptions(p, p.getQuery());
  }

  static ScanOptions buildScanOptions(
      LanceDistributedSearchInputPartition p, LanceSearchQuery base) {
    String column = base.getVectorColumn();
    if (column == null || column.isEmpty()) {
      throw new IllegalStateException(
          "vector column must be resolved on the driver before scheduling worker tasks");
    }

    boolean fallbackUnit = p.getIndexSegments().isEmpty();

    Query.Builder q = new Query.Builder().setColumn(column).setK(base.getK());
    if (base.isBatch()) {
      // One scan answers a whole chunk: the native layer walks the queries inside it, so the index
      // is opened once per chunk instead of once per query.
      q.setKeys(toFloatMatrix(base.getVectors(), base.getQueryCount(), base.getVectorDim()));
    } else {
      q.setKey(toFloatArray(base.getVector()));
    }
    // lance-core defaults use_index to true, and a fragment-restricted scan still picks up an
    // index segment covering the fragment, so a flat unit has to opt out to stay exact.
    if (fallbackUnit) {
      q.setUseIndex(false);
    }
    if (base.getDistanceType() != null && !base.getDistanceType().isEmpty()) {
      q.setDistanceType(parseDistanceType(base.getDistanceType()));
    }
    if (base.getNprobes() != null) {
      // Exact bounds, matching how lance maps a namespace query's nprobes since lance#9184. A
      // server on an older mapping treats it as a minimum; see vector-search.md.
      q.setNprobes(base.getNprobes());
    }
    if (base.getEf() != null) {
      q.setEf(base.getEf());
    }
    if (base.getRefineFactor() != null) {
      q.setRefineFactor(base.getRefineFactor());
    }

    ScanOptions.Builder b = new ScanOptions.Builder().nearest(q.build());
    // The counters behind the SQL metrics come from the scanner, and lance only records them when
    // asked. The read path sets this too (LanceFragmentScanner); without it every counter reads
    // zero and a benchmark cannot tell "storage got slower" from "native search got slower".
    b.collectStats(true);
    // ensure_not_fragment_scan rejects a fragment-restricted nearest scan unless prefilter is
    // set, so a fallback unit always sets it. Indexed units only do what the user asked; a
    // filtered query cannot reach a worker without prefilter=true.
    if (fallbackUnit || Boolean.TRUE.equals(base.getPrefilter())) {
      b.prefilter(true);
    }
    if (base.getFilter() != null) {
      b.filter(base.getFilter());
    }
    if (!base.getOutputColumns().isEmpty()) {
      b.columns(base.getOutputColumns());
    }
    if (Boolean.TRUE.equals(base.getWithRowId()) || schemaHasRowId(p.getSchema())) {
      b.withRowId(true);
    }

    if (!p.getIndexSegments().isEmpty()) {
      b.indexSegments(p.getIndexSegments());
    } else {
      b.fragmentIds(p.getFragmentIds());
    }
    return b.build();
  }

  private static boolean schemaHasRowId(StructType schema) {
    for (StructField field : schema.fields()) {
      if (field.name().equals(LanceConstant.ROW_ID)) {
        return true;
      }
    }
    return false;
  }

  private static DistanceType parseDistanceType(String name) {
    switch (LanceSearchQuery.canonicalizeDistanceType(name)) {
      case "l2":
        return DistanceType.L2;
      case "cosine":
        return DistanceType.Cosine;
      case "dot":
        return DistanceType.Dot;
      case "hamming":
        return DistanceType.Hamming;
      default:
        throw new IllegalArgumentException("Unsupported distance_type: " + name);
    }
  }

  /** Rebuilds the {@code queries x dimension} matrix lance-core takes for a batch search. */
  private static float[][] toFloatMatrix(List<Float> flat, int queries, int dimension) {
    float[][] matrix = new float[queries][dimension];
    for (int row = 0; row < queries; row++) {
      int base = row * dimension;
      for (int column = 0; column < dimension; column++) {
        matrix[row][column] = flat.get(base + column);
      }
    }
    return matrix;
  }

  private static float[] toFloatArray(List<Float> vec) {
    float[] arr = new float[vec.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = vec.get(i);
    }
    return arr;
  }

  private static String describe(LanceDistributedSearchInputPartition p) {
    if (!p.getIndexSegments().isEmpty()) {
      return "indexed segments=" + p.getIndexSegments();
    }
    return "fallback fragments=" + p.getFragmentIds();
  }
}
