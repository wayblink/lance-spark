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

import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.read.metric.LanceCustomMetrics;
import org.lance.spark.search.LanceSearchQuery.SearchType;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanceDistributedSearchScanTest {
  @Test
  void readingDriverMetricsDoesNotOpenTheDataset() {
    LanceDistributedSearchScan scan = scanForPlanning("file:/missing-metrics-only-dataset");
    Map<String, Long> metrics = driverMetrics(scan);
    assertEquals(Long.valueOf(0L), metrics.get(LanceCustomMetrics.DRIVER_PLANNING_TIME_NS));
    assertEquals(Long.valueOf(0L), metrics.get(LanceCustomMetrics.DRIVER_INDEX_DISCOVERY_TIME_NS));
  }

  @Test
  void everyReportedDriverMetricIsDeclaredAsSupported() {
    // BatchScanExec.postDriverMetrics looks each reported name up in the scan's supported metrics
    // and fails the query when one is missing, so a name reported but not declared turns every
    // successful distributed search into an exception.
    LanceDistributedSearchScan scan = scanForPlanning("file:/missing-metrics-only-dataset");
    Set<String> supported = new HashSet<>();
    for (CustomMetric metric : scan.supportedCustomMetrics()) {
      supported.add(metric.name());
    }
    CustomTaskMetric[] reported = scan.reportDriverMetrics();
    assertEquals(2, reported.length);
    for (CustomTaskMetric metric : reported) {
      assertTrue(
          supported.contains(metric.name()),
          "driver metric '"
              + metric.name()
              + "' is not in supportedCustomMetrics(), BatchScanExec would throw; supported: "
              + supported);
    }
  }

  @Test
  void driverPlanningMetricsAreSnapshotsRatherThanRepeatedAccumulation() {
    // Planning needs only this real fixture's manifest/schema/index listing; no nearest scan runs.
    LanceDistributedSearchScan scan = scanForPlanning(TestUtils.TestTable1Config.datasetUri);
    scan.planInputPartitions();
    Map<String, Long> first = driverMetrics(scan);
    assertTrue(first.getOrDefault(LanceCustomMetrics.DRIVER_PLANNING_TIME_NS, 0L) > 0L);
    assertTrue(first.getOrDefault(LanceCustomMetrics.DRIVER_INDEX_DISCOVERY_TIME_NS, 0L) > 0L);
    assertTrue(
        first.get(LanceCustomMetrics.DRIVER_PLANNING_TIME_NS)
            >= first.get(LanceCustomMetrics.DRIVER_INDEX_DISCOVERY_TIME_NS));
    scan.planInputPartitions();
    assertEquals(first, driverMetrics(scan), "cached planning and reporting must not count twice");
  }

  private static LanceDistributedSearchScan scanForPlanning(String uri) {
    LanceSearchQuery query =
        LanceSearchQuery.builder(SearchType.VECTOR)
            .tableId(Arrays.asList("ns", "table"))
            .namespaceImpl("dir")
            .readOptions(LanceSparkReadOptions.from(uri))
            .vector(Collections.singletonList(0.0f))
            .vectorColumn("x")
            .build();
    return new LanceDistributedSearchScan(new StructType(), query);
  }

  private static Map<String, Long> driverMetrics(LanceDistributedSearchScan scan) {
    Map<String, Long> values = new HashMap<>();
    for (CustomTaskMetric metric : scan.reportDriverMetrics()) {
      values.put(metric.name(), metric.value());
    }
    return values;
  }

  @Test
  void legacyStatisticsUseSegmentMetricInsteadOfNestedStorageMetric() {
    Map<String, Object> storage = new HashMap<>();
    storage.put("metric_type", "L2");

    Map<String, Object> first = new HashMap<>();
    first.put("metric_type", "COSINE");
    first.put("sub_index", storage);

    Map<String, Object> second = new HashMap<>();
    second.put("metric_type", "cosine");

    Map<String, Object> statistics = new HashMap<>();
    statistics.put("indices", Arrays.asList(first, second));

    assertEquals(
        Collections.singleton("cosine"), LanceDistributedSearchScan.indexMetricTypes(statistics));
  }

  @Test
  void blankIndexMetricsAreIgnored() {
    Map<String, Object> empty = new HashMap<>();
    empty.put("metric_type", "");
    Map<String, Object> whitespace = new HashMap<>();
    whitespace.put("metric_type", "   ");
    Map<String, Object> statistics = new HashMap<>();
    statistics.put("indices", Arrays.asList(empty, whitespace));

    assertTrue(LanceDistributedSearchScan.indexMetricTypes(statistics).isEmpty());
  }

  @Test
  void explicitNprobesSetsFixedProbeBounds() {
    LanceSearchQuery query =
        LanceSearchQuery.builder(SearchType.VECTOR)
            .tableId(Arrays.asList("ns", "table"))
            .namespaceImpl("dir")
            .vector(Collections.singletonList(0.0f))
            .vectorColumn("vector")
            .nprobes(7)
            .build();
    LanceDistributedSearchInputPartition partition =
        LanceDistributedSearchInputPartition.forFragment(
            new StructType(), query, 1, LanceBatchChunkPlan.singleQuery(), "test-scan");

    // An explicit nprobes means exactly that many probes, matching how lance maps the field on a
    // namespace query since lance#9184.
    ScanOptions options = LanceDistributedSearchColumnarPartitionReader.buildScanOptions(partition);
    Query nearest = options.getNearest().get();
    assertEquals(7, nearest.getMinimumNprobes());
    assertEquals(Optional.of(7), nearest.getMaximumNprobes(), "the probe ceiling is pinned too");
  }

  @Test
  void fallbackUnitsDisableTheIndexSoTheyStayExact() {
    LanceSearchQuery query =
        LanceSearchQuery.builder(SearchType.VECTOR)
            .tableId(Arrays.asList("ns", "table"))
            .namespaceImpl("dir")
            .vector(Collections.singletonList(0.0f))
            .vectorColumn("vector")
            .build();

    // A fragment-restricted scan still picks up an index segment covering that fragment, so the
    // flat unit has to opt out explicitly or it answers approximately. bypass_vector_index plans
    // nothing but flat units, which is why dropping this turns bypass into an ANN search.
    ScanOptions fallback =
        LanceDistributedSearchColumnarPartitionReader.buildScanOptions(
            LanceDistributedSearchInputPartition.forFragment(
                new StructType(), query, 1, LanceBatchChunkPlan.singleQuery(), "test-scan"));
    assertFalse(fallback.getNearest().get().isUseIndex(), "flat unit must not use the index");

    ScanOptions indexed =
        LanceDistributedSearchColumnarPartitionReader.buildScanOptions(
            LanceDistributedSearchInputPartition.forIndexSegment(
                new StructType(),
                query,
                UUID.randomUUID(),
                LanceBatchChunkPlan.singleQuery(),
                "test-scan"));
    assertTrue(indexed.getNearest().get().isUseIndex(), "indexed unit must use the index");
  }

  @Test
  void identifiesVectorIndexesByDetailsTypeInsteadOfConcreteIndexType() {
    assertTrue(
        LanceDistributedSearchScan.isVectorIndexTypeUrl("/lance.index.pb.VectorIndexDetails"));
    assertTrue(
        LanceDistributedSearchScan.isVectorIndexTypeUrl(
            "type.googleapis.com/lance.index.VectorIndexDetails"));
    assertFalse(
        LanceDistributedSearchScan.isVectorIndexTypeUrl(
            "type.googleapis.com/lance.index.pb.BTreeIndexDetails"));
    assertFalse(
        LanceDistributedSearchScan.isVectorIndexTypeUrl(
            "type.googleapis.com/example.MyVectorIndexDetails"));
  }

  @Test
  void anIndexIsUsableOnlyWhenItsMetricIsResolvedAndEqual() {
    // Omitted metric: any index will do, lance-core searches with the index's own metric.
    assertTrue(LanceDistributedSearchScan.indexMetricIsUsable(null, Optional.of("cosine")));
    assertTrue(LanceDistributedSearchScan.indexMetricIsUsable(null, Optional.empty()));

    // Requested metric: only a resolved, equal one qualifies.
    assertTrue(LanceDistributedSearchScan.indexMetricIsUsable("l2", Optional.of("l2")));
    assertFalse(LanceDistributedSearchScan.indexMetricIsUsable("l2", Optional.of("cosine")));
    assertFalse(
        LanceDistributedSearchScan.indexMetricIsUsable("l2", Optional.empty()),
        "an unverifiable metric must not be assumed to match: handing the segment to "
            + "indexSegments(...) makes lance-core fail the task instead of falling back");
  }

  @Test
  void segmentWithoutFragmentBitmapIsRejected() {
    assertEquals(
        new HashSet<>(Arrays.asList(1, 2)),
        LanceDistributedSearchScan.requireFragmentCoverage(
            "vec_idx", Optional.of(Arrays.asList(1, 2))));

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> LanceDistributedSearchScan.requireFragmentCoverage("vec_idx", Optional.empty()));
    assertTrue(error.getMessage().contains("without a fragment bitmap"), error.getMessage());
    assertTrue(error.getMessage().contains("vec_idx"), error.getMessage());
  }

  @Test
  void indexDiscoveryFailureIsFatalOnlyForFastSearch() {
    Exception cause = new IOException("metadata unavailable");

    LanceDistributedSearchScan.failFastSearchWhenIndexDiscoveryFails(false, cause);
    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> LanceDistributedSearchScan.failFastSearchWhenIndexDiscoveryFails(true, cause));
    assertEquals(cause, error.getCause());
    assertTrue(error.getMessage().contains("index discovery failed"));
  }
}
