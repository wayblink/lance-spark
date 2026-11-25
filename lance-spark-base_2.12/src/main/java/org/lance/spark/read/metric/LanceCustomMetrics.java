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
package org.lance.spark.read.metric;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomSumMetric;

import java.util.Arrays;

/**
 * Custom metrics for the Lance read path, displayed on the Spark UI Scan node.
 *
 * <p>Naming conventions:
 *
 * <ul>
 *   <li>Counters use {@code num*} prefix (e.g. {@code numFragmentsScanned}).
 *   <li>Durations use {@code *TimeNs} suffix and extend {@link CustomNsTimeMetric} so the UI shows
 *       formatted values like "1.2 s" instead of raw nanoseconds.
 *   <li>Sizes use {@code *Bytes} suffix (e.g. {@code numBytesRead}).
 * </ul>
 *
 * <p>Counters Lance reports through {@code ScanStats} only exist when the scanner was asked to
 * collect stats; the read path and the distributed-search path both ask for them. A scanner that
 * was not asked reports nothing rather than zeros, so a zero hit/miss pair means "not measured or
 * cache not consulted", not "everything missed".
 *
 * <p>Still gated on an upstream {@code lance-jni} surface (not implementable in pure Java today):
 * {@code numFragmentsPruned}, {@code ioWaitTimeNs}, {@code decodeTimeNs}.
 */
public final class LanceCustomMetrics {
  public static final String NUM_FRAGMENTS_SCANNED = "numFragmentsScanned";
  public static final String NUM_BATCHES_LOADED = "numBatchesLoaded";
  public static final String NUM_ROWS_SCANNED = "numRowsScanned";
  public static final String NUM_IOPS = "numIops";
  public static final String NUM_REQUESTS = "numRequests";
  public static final String NUM_BYTES_READ = "numBytesRead";
  public static final String NUM_INDICES_LOADED = "numIndicesLoaded";
  public static final String NUM_PARTS_LOADED = "numPartsLoaded";
  public static final String NUM_INDEX_COMPARISONS = "numIndexComparisons";
  public static final String NUM_INDEX_CACHE_HITS = "numIndexCacheHits";
  public static final String NUM_INDEX_CACHE_MISSES = "numIndexCacheMisses";

  public static final String NUM_BATCH_CHUNKS = "numBatchChunks";
  public static final String NUM_BATCH_CHUNK_QUERIES = "numBatchChunkQueries";
  public static final String NUM_BATCH_CHUNK_BYTES = "numBatchChunkBytes";

  public static final String DATASET_OPEN_TIME_NS = "datasetOpenTimeNs";
  public static final String SCANNER_CREATE_TIME_NS = "scannerCreateTimeNs";
  public static final String BATCH_LOAD_TIME_NS = "batchLoadTimeNs";
  public static final String DRIVER_PLANNING_TIME_NS = "driverPlanningTimeNs";
  public static final String DRIVER_INDEX_DISCOVERY_TIME_NS = "driverIndexDiscoveryTimeNs";

  private LanceCustomMetrics() {}

  // Each inner class MUST have a public no-arg constructor (Spark instantiates via reflection).

  public static class NumFragmentsScannedMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_FRAGMENTS_SCANNED;
    }

    @Override
    public String description() {
      return "number of Lance fragments scanned";
    }
  }

  public static class NumBatchesLoadedMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_BATCHES_LOADED;
    }

    @Override
    public String description() {
      return "number of Arrow batches loaded";
    }
  }

  public static class NumRowsScannedMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_ROWS_SCANNED;
    }

    @Override
    public String description() {
      return "number of rows read from storage (before filter evaluation)";
    }
  }

  public static class NumIopsMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_IOPS;
    }

    @Override
    public String description() {
      return "number of I/O operations performed";
    }
  }

  public static class NumRequestsMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_REQUESTS;
    }

    @Override
    public String description() {
      return "number of requests made to the storage layer";
    }
  }

  public static class NumBytesReadMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_BYTES_READ;
    }

    @Override
    public String description() {
      return "number of bytes read";
    }
  }

  public static class NumIndicesLoadedMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_INDICES_LOADED;
    }

    @Override
    public String description() {
      return "number of top-level indices loaded";
    }
  }

  public static class NumPartsLoadedMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_PARTS_LOADED;
    }

    @Override
    public String description() {
      return "number of index partitions loaded";
    }
  }

  public static class NumIndexComparisonsMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_INDEX_COMPARISONS;
    }

    @Override
    public String description() {
      return "number of index comparisons";
    }
  }

  public static class NumIndexCacheHitsMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_INDEX_CACHE_HITS;
    }

    @Override
    public String description() {
      return "number of native index cache hits";
    }
  }

  public static class NumIndexCacheMissesMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_INDEX_CACHE_MISSES;
    }

    @Override
    public String description() {
      return "number of native index cache misses";
    }
  }

  public static class NumBatchChunksMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_BATCH_CHUNKS;
    }

    @Override
    public String description() {
      return "number of native scans a batch search was split into (0 for a single-query search)";
    }
  }

  public static class NumBatchChunkQueriesMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_BATCH_CHUNK_QUERIES;
    }

    @Override
    public String description() {
      return "number of batch queries submitted to native scans (0 for a single-query search)";
    }
  }

  public static class NumBatchChunkBytesMetric extends CustomSumMetric {
    @Override
    public String name() {
      return NUM_BATCH_CHUNK_BYTES;
    }

    @Override
    public String description() {
      return "estimated bytes of batch queries and their result rows submitted to native scans";
    }
  }

  public static class DatasetOpenTimeNsMetric extends CustomNsTimeMetric {
    @Override
    public String name() {
      return DATASET_OPEN_TIME_NS;
    }

    @Override
    public String description() {
      return "time to open Lance dataset";
    }
  }

  public static class ScannerCreateTimeNsMetric extends CustomNsTimeMetric {
    @Override
    public String name() {
      return SCANNER_CREATE_TIME_NS;
    }

    @Override
    public String description() {
      return "time to create Lance scanner";
    }
  }

  public static class BatchLoadTimeNsMetric extends CustomNsTimeMetric {
    @Override
    public String name() {
      return BATCH_LOAD_TIME_NS;
    }

    @Override
    public String description() {
      return "time producing/importing Arrow batches (native work, I/O, IPC)";
    }
  }

  public static class DriverPlanningTimeNsMetric extends CustomNsTimeMetric {
    @Override
    public String name() {
      return DRIVER_PLANNING_TIME_NS;
    }

    @Override
    public String description() {
      return "driver time to plan distributed Lance search";
    }
  }

  public static class DriverIndexDiscoveryTimeNsMetric extends CustomNsTimeMetric {
    @Override
    public String name() {
      return DRIVER_INDEX_DISCOVERY_TIME_NS;
    }

    @Override
    public String description() {
      return "driver time to discover and select vector index";
    }
  }

  private static final CustomMetric[] ALL_METRICS = {
    new NumFragmentsScannedMetric(),
    new NumBatchesLoadedMetric(),
    new NumRowsScannedMetric(),
    new NumIopsMetric(),
    new NumRequestsMetric(),
    new NumBytesReadMetric(),
    new NumIndicesLoadedMetric(),
    new NumPartsLoadedMetric(),
    new NumIndexComparisonsMetric(),
    new NumIndexCacheHitsMetric(),
    new NumIndexCacheMissesMetric(),
    new DatasetOpenTimeNsMetric(),
    new ScannerCreateTimeNsMetric(),
    new BatchLoadTimeNsMetric(),
  };

  private static final CustomMetric[] BATCH_SEARCH_METRICS = {
    new NumBatchChunksMetric(), new NumBatchChunkQueriesMetric(), new NumBatchChunkBytesMetric(),
  };

  /**
   * Search-only metrics: the driver timings are never emitted by a task reader, the batch-chunk
   * counters are never emitted by the read path, and the read path must not declare counters it
   * cannot report.
   */
  public static CustomMetric[] distributedSearchMetrics() {
    CustomMetric[] metrics =
        Arrays.copyOf(ALL_METRICS, ALL_METRICS.length + 2 + BATCH_SEARCH_METRICS.length);
    // Appended after the copied array: overwriting from index 0 would leave the tail null, and
    // DataSourceV2ScanExecBase fails the query on a null metric.
    int next = ALL_METRICS.length;
    metrics[next++] = new DriverPlanningTimeNsMetric();
    metrics[next++] = new DriverIndexDiscoveryTimeNsMetric();
    for (CustomMetric metric : BATCH_SEARCH_METRICS) {
      metrics[next++] = metric;
    }
    return metrics;
  }

  /** Returns all supported custom metrics, used by LanceScan.supportedCustomMetrics(). */
  public static CustomMetric[] allMetrics() {
    return ALL_METRICS.clone();
  }
}
