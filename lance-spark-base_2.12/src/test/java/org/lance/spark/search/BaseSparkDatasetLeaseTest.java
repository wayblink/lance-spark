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

import org.lance.spark.LanceDataset;
import org.lance.spark.internal.DatasetLease;
import org.lance.spark.internal.DatasetLeaseSettings;
import org.lance.spark.read.metric.LanceCustomMetrics;
import org.lance.spark.utils.Utils;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.adaptive.QueryStageExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The executor-side dataset lease and the driver-side planning cache, exercised through real
 * distributed searches.
 *
 * <p>What has to hold: a query does not open one dataset per task; turning the lease off restores
 * exactly that; both modes answer the same rows; a second query on the same pinned version skips
 * index discovery; and a pinned version keeps reading its own snapshot after an append or a delete.
 */
public abstract class BaseSparkDatasetLeaseTest {
  private static final String CATALOG_NAME = "lance_lease";
  private static final String LEASE_ENABLED = DatasetLeaseSettings.ENABLED_CONF;

  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  void setup() throws Exception {
    spark =
        SparkSession.builder()
            .appName("lance-dataset-lease-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG_NAME + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE " + CATALOG_NAME + ".default");
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    // Neither cache may leak state from another test.
    DatasetLease.resetForTest();
    LanceDistributedSearchScan.metadataCacheForTest().clear();
  }

  @AfterEach
  void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
    DatasetLease.resetForTest();
    LanceDistributedSearchScan.metadataCacheForTest().clear();
  }

  /** Five fragments of one row each, so planning emits five units and therefore five tasks. */
  private String createFiveFragmentTable() {
    String fullName = CATALOG_NAME + ".default.lease_vec";
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, body STRING, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    for (int i = 0; i < 5; i++) {
      spark.sql(
          "INSERT INTO "
              + fullName
              + " VALUES ("
              + i
              + ", 'row-"
              + i
              + "', array("
              + i
              + ".0, "
              + i
              + ".0, "
              + i
              + ".0, "
              + i
              + ".0))");
    }
    return fullName;
  }

  /** The version the five inserts produced, i.e. the snapshot later reads have to stay on. */
  private long currentVersion(String table) throws Exception {
    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(CATALOG_NAME);
    LanceDataset lanceTable =
        (LanceDataset)
            catalog.loadTable(
                Identifier.of(
                    new String[] {"default"}, table.substring(table.lastIndexOf('.') + 1)));
    try (org.lance.Dataset dataset = Utils.openDatasetBuilder(lanceTable.readOptions()).build()) {
      return dataset.getVersion().getId();
    }
  }

  private static void assumeNamedArguments(SparkSession spark) {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "VECTOR_SEARCH named arguments do not exist on Spark 3.4");
  }

  private static String searchSql(String table, int k) {
    return "SELECT id, _distance FROM VECTOR_SEARCH('"
        + table
        + "', array(0.0, 0.0, 0.0, 0.0), "
        + k
        + ") ORDER BY _distance, id";
  }

  private static String versionedSearchSql(String table, int k, int version) {
    return "SELECT id, _distance FROM VECTOR_SEARCH(table => '"
        + table
        + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => "
        + k
        + ", version => "
        + version
        + ") ORDER BY _distance, id";
  }

  /** Runs one action and returns the summed custom task metrics plus the task count. */
  private Cumulative run(String sql) throws Exception {
    LanceTaskMetricsListener metrics = new LanceTaskMetricsListener();
    AtomicLong tasks = new AtomicLong();
    SparkListener counter =
        new SparkListener() {
          @Override
          public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
            if (taskEnd.taskInfo() != null) {
              tasks.incrementAndGet();
            }
          }
        };
    spark.sparkContext().addSparkListener(metrics);
    spark.sparkContext().addSparkListener(counter);
    try {
      Dataset<Row> result = spark.sql(sql);
      List<Row> rows = new ArrayList<>(result.collectAsList());
      spark.sparkContext().listenerBus().waitUntilEmpty(10_000);
      return new Cumulative(rows, metrics.metricValues(), tasks.get(), driverMetrics(result));
    } finally {
      spark.sparkContext().removeSparkListener(metrics);
      spark.sparkContext().removeSparkListener(counter);
    }
  }

  private static final class Cumulative {
    private final List<Row> rows;
    private final Map<String, Long> taskMetrics;
    private final long tasks;
    private final Map<String, Long> driverMetrics;

    Cumulative(
        List<Row> rows,
        Map<String, Long> taskMetrics,
        long tasks,
        Map<String, Long> driverMetrics) {
      this.rows = rows;
      this.taskMetrics = taskMetrics;
      this.tasks = tasks;
      this.driverMetrics = driverMetrics;
    }

    long taskMetric(String name) {
      return taskMetrics.getOrDefault(name, 0L);
    }

    long driverMetric(String name) {
      return driverMetrics == null ? 0L : driverMetrics.getOrDefault(name, 0L);
    }

    List<String> ids() {
      List<String> values = new ArrayList<>();
      for (Row row : rows) {
        values.add(row.getInt(0) + "@" + row.getFloat(1));
      }
      return values;
    }
  }

  private static Map<String, Long> driverMetrics(Dataset<Row> result) {
    return driverMetricsOf(result.queryExecution().executedPlan());
  }

  private static Map<String, Long> driverMetricsOf(SparkPlan plan) {
    if (plan instanceof AdaptiveSparkPlanExec) {
      return driverMetricsOf(((AdaptiveSparkPlanExec) plan).executedPlan());
    }
    if (plan instanceof QueryStageExec) {
      return driverMetricsOf(((QueryStageExec) plan).plan());
    }
    if (plan instanceof BatchScanExec) {
      BatchScanExec exec = (BatchScanExec) plan;
      if (exec.scan() instanceof LanceDistributedSearchScan) {
        Map<String, Long> values = new HashMap<>();
        for (Map.Entry<String, SQLMetric> metric :
            JavaConverters.mapAsJavaMap(exec.metrics()).entrySet()) {
          values.put(metric.getKey(), metric.getValue().value());
        }
        return values;
      }
    }
    for (SparkPlan child : JavaConverters.seqAsJavaList(plan.children())) {
      Map<String, Long> found = driverMetricsOf(child);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  // ---------------------------------------------------------------------------- tests

  @Test
  void aQueryOpensTheDatasetOnceAndLeasesItToTheOtherTasks() throws Exception {
    String table = createFiveFragmentTable();
    Cumulative result = run(searchSql(table, 5));

    assertEquals(5, result.rows.size());
    // Every search task reports either an open or a reuse, so the sum is the number of tasks that
    // had to hold a dataset - five units, one of which opened and four of which reused.
    assertEquals(
        5L,
        result.taskMetric(LanceCustomMetrics.NUM_DATASET_OPENS)
            + result.taskMetric(LanceCustomMetrics.NUM_DATASET_LEASE_HITS),
        "five search tasks must account for five acquisitions, metrics: " + result.taskMetrics);
    assertEquals(
        1L,
        result.taskMetric(LanceCustomMetrics.NUM_DATASET_OPENS),
        "one open must serve every task on this executor, metrics: " + result.taskMetrics);
    assertEquals(
        4L,
        result.taskMetric(LanceCustomMetrics.NUM_DATASET_LEASE_HITS),
        "the other four tasks must reuse that open, metrics: " + result.taskMetrics);
  }

  @Test
  void turningTheLeaseOffOpensOncePerTaskAgain() throws Exception {
    spark.conf().set(LEASE_ENABLED, "false");
    try {
      String table = createFiveFragmentTable();
      Cumulative result = run(searchSql(table, 5));

      assertEquals(
          5L,
          result.taskMetric(LanceCustomMetrics.NUM_DATASET_OPENS),
          "with the lease off every task opens its own dataset, which is the pre-lease cost: "
              + result.taskMetrics);
      assertEquals(0L, result.taskMetric(LanceCustomMetrics.NUM_DATASET_LEASE_HITS));
    } finally {
      spark.conf().set(LEASE_ENABLED, "true");
    }
  }

  @Test
  void bothLeaseModesAnswerTheSameRows() throws Exception {
    String table = createFiveFragmentTable();
    List<String> leased = run(searchSql(table, 3)).ids();
    spark.conf().set(LEASE_ENABLED, "false");
    List<String> unleased;
    try {
      unleased = run(searchSql(table, 3)).ids();
    } finally {
      spark.conf().set(LEASE_ENABLED, "true");
    }
    assertEquals(unleased, leased, "a reused dataset must not change the answer");
    assertEquals(3, leased.size());
  }

  @Test
  void aSecondQueryOnTheSameVersionSkipsIndexDiscovery() throws Exception {
    // The executed-plan copy is the one that posted the driver metrics; AQE may plan a stage from
    // a fresh copy, which would report zero for both runs.
    spark.conf().set("spark.sql.adaptive.enabled", "false");
    String table = createFiveFragmentTable();
    Cumulative first = run(searchSql(table, 3));
    Cumulative second = run(searchSql(table, 3));

    assertTrue(
        first.driverMetric(LanceCustomMetrics.DRIVER_INDEX_DISCOVERY_TIME_NS) > 0L,
        "the first query has to discover the index, metrics: " + first.driverMetrics);
    assertEquals(
        0L,
        second.driverMetric(LanceCustomMetrics.DRIVER_INDEX_DISCOVERY_TIME_NS),
        "a second query on the same version must reuse the planning cache, metrics: "
            + second.driverMetrics);
    assertEquals(first.ids(), second.ids(), "the cached plan must return the same rows");
    assertTrue(
        LanceDistributedSearchScan.metadataCacheForTest().stats().hits() >= 1,
        "the metadata cache must report the hit: "
            + LanceDistributedSearchScan.metadataCacheForTest().stats());
  }

  @Test
  void aPinnedVersionKeepsReadingItsOwnSnapshotAfterAnAppend() throws Exception {
    assumeNamedArguments(spark);
    String table = createFiveFragmentTable();
    int pinnedVersion = (int) currentVersion(table);
    List<String> pinned = run(versionedSearchSql(table, 5, pinnedVersion)).ids();

    spark.sql("INSERT INTO " + table + " VALUES (9, 'row-9', array(9.0, 9.0, 9.0, 9.0))");
    List<String> pinnedAgain = run(versionedSearchSql(table, 5, pinnedVersion)).ids();

    assertEquals(5, pinned.size(), "version 1 had five rows");
    assertEquals(
        pinned, pinnedAgain, "a cached plan must never mix the pinned version with a later one");
    List<String> latest = run(searchSql(table, 6)).ids();
    assertEquals(6, latest.size(), "the unpinned query has to see the new row");
  }

  @Test
  void aPinnedVersionKeepsReadingItsOwnSnapshotAfterADelete() throws Exception {
    assumeNamedArguments(spark);
    String table = createFiveFragmentTable();
    int pinnedVersion = (int) currentVersion(table);
    List<String> pinned = run(versionedSearchSql(table, 5, pinnedVersion)).ids();

    spark.sql("DELETE FROM " + table + " WHERE id = 1");
    List<String> pinnedAgain = run(versionedSearchSql(table, 5, pinnedVersion)).ids();

    assertTrue(pinned.stream().anyMatch(row -> row.startsWith("1@")), "version 1 contained id 1");
    assertEquals(
        pinned,
        pinnedAgain,
        "the pinned version still has the deleted row; a cached plan must not drop it");
    List<String> latest = run(searchSql(table, 5)).ids();
    assertTrue(
        latest.stream().noneMatch(row -> row.startsWith("1@")),
        "the unpinned query must not see the deleted row: " + latest);
  }

  @Test
  void theMetadataCacheIsBoundedAndEvictsTheColdestVersion() throws Exception {
    String table = createFiveFragmentTable();
    spark.conf().set(SearchPlanCache.ENTRIES_CONF, "1");
    try {
      assertNotNull(run(searchSql(table, 3)).rows);
      assertEquals(1, LanceDistributedSearchScan.metadataCacheForTest().size());
      spark.sql("INSERT INTO " + table + " VALUES (9, 'row-9', array(9.0, 9.0, 9.0, 9.0))");
      assertNotNull(run(searchSql(table, 4)).rows);
      assertEquals(
          1,
          LanceDistributedSearchScan.metadataCacheForTest().size(),
          "the entry bound must hold across versions");
    } finally {
      spark
          .conf()
          .set(SearchPlanCache.ENTRIES_CONF, String.valueOf(SearchPlanCache.DEFAULT_ENTRIES));
    }
  }
}
