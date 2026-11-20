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

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
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

  private final LanceDistributedSearchInputPartition partition;
  private final LanceReadMetricsTracker metricsTracker = new LanceReadMetricsTracker();
  private ExecutorNamespace executorNamespace;
  private Dataset dataset;
  private LanceScanner scanner;
  private ArrowReader reader;
  private ColumnarBatch currentBatch;
  private boolean finished;
  private boolean statsCollected;

  public LanceDistributedSearchColumnarPartitionReader(
      LanceDistributedSearchInputPartition partition) {
    this.partition = partition;
  }

  @Override
  public boolean next() throws IOException {
    if (finished) {
      return false;
    }
    if (reader == null) {
      openReader();
    }
    long batchStart = System.nanoTime();
    boolean hasNext = reader.loadNextBatch();
    if (hasNext) {
      metricsTracker.addBatchLoadTimeNs(System.nanoTime() - batchStart);
      metricsTracker.addNumBatchesLoaded(1);
      currentBatch =
          LanceSearchColumnarPartitionReader.toColumnarBatch(
              reader.getVectorSchemaRoot(), partition.getSchema());
      return true;
    }
    // Spark samples the reader's metrics before it calls close() (DataSourceRDD's task-completion
    // listener), so the counters have to be read while the scan still exists and before this
    // returns false. Collecting them in close() alone leaves every counter at zero.
    captureScanStats();
    finished = true;
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    // Fallback for a reader closed before its scan ran dry (cancellation, early stop): the
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
    first = closeQuietly(reader, first);
    first = closeQuietly(scanner, first);
    first = closeQuietly(dataset, first);
    first = closeQuietly(executorNamespace, first);
    if (first != null) {
      throw new IOException("Failed to close LanceDistributedSearchColumnarPartitionReader", first);
    }
  }

  /**
   * Publishes the scanner's counters. Scan stats are cumulative for the whole scan, so they are
   * collected exactly once; reading diagnostics must never turn a finished search into a failed
   * task.
   */
  private void captureScanStats() {
    if (scanner == null || statsCollected) {
      return;
    }
    statsCollected = true;
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
    return metricsTracker.currentMetricsValues();
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

  private void openReader() throws IOException {
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
    ScanOptions opts = buildScanOptions(partition);
    long scanStart = System.nanoTime();
    try {
      scanner = dataset.newScan(opts);
      reader = scanner.scanBatches();
      metricsTracker.addScannerCreateTimeNs(System.nanoTime() - scanStart);
    } catch (Exception e) {
      throw new IOException(
          "Failed to open distributed search scan for partition: " + describe(partition), e);
    }
  }

  static ScanOptions buildScanOptions(LanceDistributedSearchInputPartition p) {
    LanceSearchQuery base = p.getQuery();
    String column = base.getVectorColumn();
    if (column == null || column.isEmpty()) {
      throw new IllegalStateException(
          "vector column must be resolved on the driver before scheduling worker tasks");
    }

    boolean fallbackUnit = p.getIndexSegments().isEmpty();

    Query.Builder q = new Query.Builder().setColumn(column).setK(base.getK());
    if (base.isBatch()) {
      // One scan answers the whole batch: the native layer walks the queries inside it, so the
      // index is opened once per task instead of once per query.
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
