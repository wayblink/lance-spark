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

import org.lance.ipc.ScanOptions;
import org.lance.spark.read.metric.LanceCustomMetrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for distributed VECTOR_SEARCH (the path through {@link
 * LanceDistributedSearchTable}).
 *
 * <p>This class covers the fallback-only path: with no vector index on the table, {@link
 * LanceDistributedSearchScan} emits one fallback partition per fragment and each Spark task runs
 * flat KNN. Indexed units are covered by {@code BaseSparkDistributedVectorSearchIndexTest}, which
 * builds real index segments through the Java API.
 */
public abstract class BaseSparkDistributedVectorSearchTest {
  private static final String CATALOG_NAME = "lance_dist_search";
  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-distributed-vector-search-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG_NAME + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE " + CATALOG_NAME + ".default");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  /**
   * Build a 5-fragment table with no vector index. The planner emits five fallback units, one per
   * fragment. Each fragment has a single row; vectors are spaced so the closest to (0,0,0,0) is id
   * 0, then 1, then 2, etc.
   */
  private String createFiveFragmentTable() {
    String fullName = CATALOG_NAME + ".default.dist_vec";
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    spark.sql("INSERT INTO " + fullName + " VALUES (0, array(0.0, 0.0, 0.0, 0.0))");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, array(1.0, 1.0, 1.0, 1.0))");
    spark.sql("INSERT INTO " + fullName + " VALUES (2, array(2.0, 2.0, 2.0, 2.0))");
    spark.sql("INSERT INTO " + fullName + " VALUES (3, array(3.0, 3.0, 3.0, 3.0))");
    spark.sql("INSERT INTO " + fullName + " VALUES (4, array(4.0, 4.0, 4.0, 4.0))");
    return fullName;
  }

  @Test
  void distributedAndSinglePartitionAgreeOnTopK() {
    String fullName = createFiveFragmentTable();
    String sql =
        "SELECT id, _distance FROM VECTOR_SEARCH('"
            + fullName
            + "', array(0.0, 0.0, 0.0, 0.0), 5) ORDER BY _distance, id";

    spark.conf().set("spark.sql.lance.search.distributed.enabled", "false");
    List<Row> single = new ArrayList<>(spark.sql(sql).collectAsList());

    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> distributed = new ArrayList<>(spark.sql(sql).collectAsList());

    Comparator<Row> byIdThenDist =
        Comparator.<Row, Integer>comparing(r -> r.getInt(0))
            .thenComparingDouble(r -> (double) r.getFloat(1));
    single.sort(byIdThenDist);
    distributed.sort(byIdThenDist);

    assertEquals(single.size(), distributed.size(), "result row count must match");
    for (int i = 0; i < single.size(); i++) {
      assertEquals(
          single.get(i).getInt(0), distributed.get(i).getInt(0), "row " + i + " id mismatch");
      assertEquals(
          single.get(i).getFloat(1),
          distributed.get(i).getFloat(1),
          1e-3f,
          "row " + i + " _distance mismatch");
    }
  }

  @Test
  void distributedReturnsExactlyKRows() {
    String fullName = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> rows =
        spark
            .sql("SELECT id FROM VECTOR_SEARCH('" + fullName + "', array(0.0, 0.0, 0.0, 0.0), 3)")
            .collectAsList();
    assertEquals(3, rows.size());
  }

  @Test
  void distributedKLargerThanRowsReturnsAllRows() {
    String fullName = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> rows =
        spark
            .sql("SELECT id FROM VECTOR_SEARCH('" + fullName + "', array(0.0, 0.0, 0.0, 0.0), 100)")
            .collectAsList();
    assertEquals(5, rows.size());
  }

  @Test
  void distributedEmptyTableReturnsZeroRows() {
    String fullName = CATALOG_NAME + ".default.empty_vec";
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> rows =
        spark
            .sql("SELECT id FROM VECTOR_SEARCH('" + fullName + "', array(0.0, 0.0, 0.0, 0.0), 5)")
            .collectAsList();
    assertEquals(0, rows.size());
  }

  @Test
  void distributedNearestRowIsClosest() {
    String fullName = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> rows =
        spark
            .sql(
                "SELECT id, _distance FROM VECTOR_SEARCH('"
                    + fullName
                    + "', array(0.0, 0.0, 0.0, 0.0), 1)")
            .collectAsList();
    assertEquals(1, rows.size());
    assertEquals(0, rows.get(0).getInt(0), "closest id to origin should be 0");
    assertEquals(0.0f, rows.get(0).getFloat(1), 1e-4f);
  }

  @Test
  void distanceBoundsAreRejectedByDistributedSearch() {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "Spark 3.4 table-valued functions do not support named arguments");
    String fullName = createFiveFragmentTable();
    String sql =
        "SELECT id, _distance FROM VECTOR_SEARCH(table => '"
            + fullName
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 5, "
            + "lower_bound => 0.5, upper_bound => 5.0)";

    spark.conf().set("spark.sql.lance.search.distributed.enabled", "false");
    List<Row> rows = spark.sql(sql).collectAsList();
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals(4.0f, rows.get(0).getFloat(1), 1e-4f);

    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    Exception error = assertThrows(Exception.class, () -> spark.sql(sql).collectAsList());
    assertTrue(
        rootMessage(error)
            .contains("Distributed VECTOR_SEARCH does not support lower_bound or upper_bound"),
        rootMessage(error));
  }

  @Test
  void aFilterRequiresAnExplicitPrefilterChoice() {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "Spark 3.4 table-valued functions do not support named arguments");
    String fullName = createFiveFragmentTable();
    String filtered =
        "SELECT id FROM VECTOR_SEARCH(table => '"
            + fullName
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 1, filter => 'id = 4'";

    // Namespace execution defaults to prefilter=false: it takes the top 1 (id 0), then applies
    // the filter, so nothing comes back. This is the semantics a distributed plan cannot offer,
    // because a fragment-restricted scan has to prefilter.
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "false");
    assertEquals(
        0,
        spark.sql(filtered + ")").collectAsList().size(),
        "namespace execution post-filters the top k");

    // Silently prefiltering instead would return id 4 here, so the query is rejected until the
    // user says which semantics they want.
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    Exception error =
        assertThrows(Exception.class, () -> spark.sql(filtered + ")").collectAsList());
    assertTrue(rootMessage(error).contains("requires prefilter=true"), rootMessage(error));

    List<Row> prefiltered = spark.sql(filtered + ", prefilter => true)").collectAsList();
    assertEquals(1, prefiltered.size(), "prefilter=true yields the true filtered top k");
    assertEquals(4, prefiltered.get(0).getInt(0));
  }

  @Test
  void tiedDistancesPaginateTheSameWayEveryRun() {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "Spark 3.4 table-valued functions do not support named arguments");
    // Identical vectors spread over several fragments: every row ties on `_distance`, so sorting
    // on distance alone leaves the k and offset boundaries up to task arrival order and the two
    // pages overlap or skip rows between runs. lance-core sorts by (distance, row id).
    String fullName = CATALOG_NAME + ".default.dist_ties";
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    for (int fragment = 0; fragment < 4; fragment++) {
      int start = fragment * 3;
      spark.sql(
          "INSERT INTO "
              + fullName
              + " SELECT CAST(id AS INT), array(1.0, 1.0, 1.0, 1.0) FROM range("
              + start
              + ", "
              + (start + 3)
              + ", 1, 1)");
    }

    String page =
        "SELECT id FROM VECTOR_SEARCH(table => '"
            + fullName
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 5";
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");

    List<Integer> firstPage = idsOf(page + ")");
    List<Integer> secondPage = idsOf(page + ", offset => 5)");
    assertEquals(5, firstPage.size());
    assertEquals(5, secondPage.size());
    for (int run = 0; run < 4; run++) {
      assertEquals(firstPage, idsOf(page + ")"), "page 1 must not move between runs");
      assertEquals(secondPage, idsOf(page + ", offset => 5)"), "page 2 must not move");
    }
    List<Integer> combined = new ArrayList<>(firstPage);
    combined.addAll(secondPage);
    assertEquals(
        combined.size(),
        new java.util.HashSet<>(combined).size(),
        "the two pages must not repeat a row: " + combined);
  }

  /**
   * The distributed search tasks must report the same scan counters and timings as the read path.
   * Without them a benchmark that differs by 20% between runs cannot tell "storage got slower" from
   * "native search got slower" (card #3089).
   */
  @Test
  void distributedSearchReportsScanMetrics() throws Exception {
    String fullName = createFiveFragmentTable();
    LanceTaskMetricsListener listener = new LanceTaskMetricsListener();
    spark.sparkContext().addSparkListener(listener);
    try {
      spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
      spark
          .sql("SELECT id FROM VECTOR_SEARCH('" + fullName + "', array(0.0, 0.0, 0.0, 0.0), 3)")
          .collectAsList();
      spark.sparkContext().listenerBus().waitUntilEmpty(5000);
    } finally {
      spark.sparkContext().removeSparkListener(listener);
    }

    Map<String, Long> metrics = listener.metricValues();
    assertTrue(metrics.getOrDefault(LanceCustomMetrics.NUM_IOPS, 0L) > 0, "numIops: " + metrics);
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_REQUESTS, 0L) > 0, "numRequests: " + metrics);
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_BYTES_READ, 0L) > 0,
        "numBytesRead: " + metrics);
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_BATCHES_LOADED, 0L) >= 1,
        "numBatchesLoaded: " + metrics);
    // Derived from Lance's free-form scan counter rather than a documented field: this assertion
    // is what catches an upstream rename that would silently zero it. Lance's fragments_scanned is
    // not mapped here - it counts physical fragment scans and reports 2 for one fragment.
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_ROWS_SCANNED, 0L) > 0,
        "numRowsScanned: " + metrics);
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.DATASET_OPEN_TIME_NS, 0L) > 0,
        "datasetOpenTimeNs: " + metrics);
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.SCANNER_CREATE_TIME_NS, 0L) > 0,
        "scannerCreateTimeNs: " + metrics);
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.BATCH_LOAD_TIME_NS, 0L) > 0,
        "batchLoadTimeNs: " + metrics);
  }

  @Test
  void distributedRequiredProjectionReachesSearchScan() {
    String fullName = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    Dataset<Row> result =
        spark.sql(
            "SELECT id, _distance FROM VECTOR_SEARCH(table => '"
                + fullName
                + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 2)");
    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(0, rows.get(0).getInt(0));
    Scan scan = findSearchScan(result);
    assertEquals(
        java.util.Arrays.asList("id", "_distance", "_rowid"),
        java.util.Arrays.asList(scan.readSchema().fieldNames()));

    // The scan schema only proves what Spark sees; the payload read is saved by the native column
    // list the workers send to Lance, which stays whole if pruneColumns stops pruning the query.
    InputPartition[] units = ((Batch) scan).planInputPartitions();
    assertTrue(units.length > 0, "expected at least one distributed search unit");
    for (InputPartition unit : units) {
      ScanOptions options =
          LanceDistributedSearchColumnarPartitionReader.buildScanOptions(
              (LanceDistributedSearchInputPartition) unit);
      assertEquals(
          java.util.Arrays.asList("id", "_distance"),
          options.getColumns().orElse(java.util.Collections.emptyList()),
          "native columns requested by one search unit");
      assertTrue(options.isWithRowId(), "row id is fetched by flag, not as a payload column");
    }
  }

  private static Scan findSearchScan(Dataset<Row> dataset) {
    Scan scan = findSearchScanOrNull(dataset.queryExecution().executedPlan());
    assertTrue(
        scan != null,
        "VECTOR_SEARCH BatchScanExec not found in " + dataset.queryExecution().executedPlan());
    return scan;
  }

  private static Scan findSearchScanOrNull(SparkPlan plan) {
    if (plan instanceof BatchScanExec) {
      Scan scan = ((BatchScanExec) plan).scan();
      if (scan.readSchema().fieldNames().length > 0
          && java.util.Arrays.asList(scan.readSchema().fieldNames()).contains("_distance")) {
        return scan;
      }
    }
    if (plan instanceof AdaptiveSparkPlanExec) {
      return findSearchScanOrNull(((AdaptiveSparkPlanExec) plan).executedPlan());
    }
    if (plan instanceof QueryStageExec) {
      return findSearchScanOrNull(((QueryStageExec) plan).plan());
    }
    for (SparkPlan child : scala.collection.JavaConverters.seqAsJavaList(plan.children())) {
      Scan result = findSearchScanOrNull(child);
      if (result != null) return result;
    }
    return null;
  }

  /**
   * The driver-only durations must actually reach the scan node. Spark's {@code
   * BatchScanExec.postDriverMetrics} looks every reported name up in {@code metrics}, which is
   * built from {@code supportedCustomMetrics()}, and throws on a query that otherwise succeeded
   * when a name is missing: a mismatch here fails every distributed search, not just the metrics
   * column.
   */
  @Test
  void distributedSearchReportsDriverPlanningMetricsOnTheScanNode() {
    String fullName = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    // The executed-plan copy of the scan node is the one that posted the metrics; AQE may plan a
    // stage from a fresh copy, which would start at zero.
    spark.conf().set("spark.sql.adaptive.enabled", "false");

    Dataset<Row> result =
        spark.sql("SELECT id FROM VECTOR_SEARCH('" + fullName + "', array(0.0, 0.0, 0.0, 0.0), 3)");
    assertEquals(3, result.collectAsList().size());

    Map<String, Long> driverMetrics = driverMetricsOf(result.queryExecution().executedPlan());
    assertNotNull(driverMetrics, "no BatchScanExec with LanceDistributedSearchScan in the plan");
    assertTrue(
        driverMetrics.getOrDefault(LanceCustomMetrics.DRIVER_PLANNING_TIME_NS, 0L) > 0L,
        "driverPlanningTimeNs: " + driverMetrics);
    assertTrue(
        driverMetrics.getOrDefault(LanceCustomMetrics.DRIVER_INDEX_DISCOVERY_TIME_NS, 0L) > 0L,
        "driverIndexDiscoveryTimeNs: " + driverMetrics);
  }

  /**
   * The scan node's own metric map, which is what {@code postDriverMetrics} updated, or null when
   * the plan holds no distributed search scan.
   */
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

  // ------------------------------------------------- query_id and batch chunking

  /**
   * The contract a chunked batch rests on: `query_id` is the query's position in the input
   * sequence, so a business id column only decides the order and never becomes the id. NULLs and
   * duplicates stay legal - they make the order among themselves unspecified, which is why the
   * assertion is a bijection rather than a fixed mapping.
   */
  @Test
  void batchQueryIdsAreInputPositionsNotBusinessIds() {
    assumeNamedArguments();
    String table = createFiveFragmentTable();
    String queries = CATALOG_NAME + ".default.chunk_queries";
    spark.sql("DROP TABLE IF EXISTS " + queries + " PURGE");
    // The id column is nullable on purpose: a batch whose ordering column has NULLs must still
    // number its queries 0..N-1. Rows are inserted in reverse id order so insert order cannot
    // explain the numbering.
    spark.sql(
        "CREATE TABLE "
            + queries
            + " (qid INT, vec ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vec.arrow.fixed-size-list.size' = '4')");
    spark.sql("INSERT INTO " + queries + " SELECT 3, array(3.0, 3.0, 3.0, 3.0)");
    spark.sql("INSERT INTO " + queries + " SELECT 1, array(1.0, 1.0, 1.0, 1.0)");
    spark.sql("INSERT INTO " + queries + " SELECT 1, array(4.0, 4.0, 4.0, 4.0)");
    spark.sql("INSERT INTO " + queries + " SELECT NULL, array(0.0, 0.0, 0.0, 0.0)");

    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> rows =
        spark
            .sql(
                "SELECT query_id, id, _distance FROM VECTOR_SEARCH(table => '"
                    + table
                    + "', query_table => '"
                    + queries
                    + "', query_id_column => 'qid', query_vector_column => 'vec', k => 2)"
                    + " ORDER BY query_id, _distance, id")
            .collectAsList();

    assertEquals(8, rows.size(), "four queries times k rows");
    // Spark orders NULLs first ascending, so the NULL-id row is query 0; which of the two qid=1
    // rows is query 1 and which is 2 is not specified, and this test must not depend on it.
    for (int query = 0; query < 4; query++) {
      assertEquals(
          Integer.valueOf(query),
          rows.get(query * 2).getInt(0),
          "query ids must be 0..3 in input order, saw " + rows);
    }
    assertEquals(
        0,
        rows.get(0).getInt(1),
        "query 0 is the NULL-id row (array of zeros), whose nearest row is id 0");
    assertEquals(
        rankedIdsOf(singleQuerySearch(table, 2, constantVector(0.0))),
        idsOfQuery(rows, 0),
        "query 0 must answer what searching its vector alone answers");

    List<List<Integer>> answered = new ArrayList<>();
    for (int query = 1; query < 4; query++) {
      answered.add(idsOfQuery(rows, query));
    }
    List<List<Integer>> expected = new ArrayList<>();
    for (double center : new double[] {1.0, 3.0, 4.0}) {
      expected.add(rankedIdsOf(singleQuerySearch(table, 2, constantVector(center))));
    }
    Comparator<List<Integer>> byContents = Comparator.comparing(Object::toString);
    answered.sort(byContents);
    expected.sort(byContents);
    assertEquals(
        expected,
        answered,
        "queries 1..3 must be exactly the three remaining vectors, in some order");
  }

  /**
   * A NULL query vector is skipped, and skipping must not renumber the queries after it: a caller
   * maps `query_id` back to the positions of the rows it kept.
   */
  @Test
  void aNullQueryVectorIsSkippedWithoutShiftingNumbering() {
    assumeNamedArguments();
    String table = createFiveFragmentTable();
    String queries = CATALOG_NAME + ".default.null_vec_queries";
    spark.sql("DROP TABLE IF EXISTS " + queries + " PURGE");
    spark.sql("CREATE TABLE " + queries + " (qid INT NOT NULL, vec ARRAY<FLOAT>) USING lance");
    spark.sql("INSERT INTO " + queries + " SELECT 0, array(0.0, 0.0, 0.0, 0.0)");
    spark.sql("INSERT INTO " + queries + " SELECT 1, NULL");
    spark.sql("INSERT INTO " + queries + " SELECT 2, array(2.0, 2.0, 2.0, 2.0)");

    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    List<Row> rows =
        spark
            .sql(
                "SELECT query_id, id FROM VECTOR_SEARCH(table => '"
                    + table
                    + "', query_table => '"
                    + queries
                    + "', query_id_column => 'qid', query_vector_column => 'vec', k => 1)"
                    + " ORDER BY query_id")
            .collectAsList();

    assertEquals(2, rows.size(), "the two non-null queries answer, the null one is skipped");
    assertEquals(0, rows.get(0).getInt(0), "numbering starts at 0 and stays contiguous");
    assertEquals(1, rows.get(1).getInt(0), "the row after the skipped query is not renumbered");
    assertEquals(0, rows.get(0).getInt(1), "query 0 is the zero vector, nearest row is id 0");
    assertEquals(2, rows.get(1).getInt(1), "query 1 is the 2-vector, nearest row is id 2");
  }

  /**
   * The whole point of chunking: a batch cut into several native scans must answer exactly what the
   * same batch answers in one scan, `query_id` included. A four-query batch over a five-fragment
   * table needs 224 estimated bytes, so a 112-byte budget forces two queries per scan.
   */
  @Test
  void aChunkedBatchAnswersExactlyWhatAnUnchunkedBatchAnswers() throws Exception {
    assumeNamedArguments();
    String table = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    String sql =
        "SELECT query_id, id, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', queries => array('0.0,0.0,0.0,0.0', '1.0,1.0,1.0,1.0', "
            + "'2.0,2.0,2.0,2.0', '4.0,4.0,4.0,4.0'), k => 2)"
            + " ORDER BY query_id, _distance, id";

    spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "67108864");
    List<Row> unchunked = spark.sql(sql).collectAsList();

    LanceTaskMetricsListener listener = new LanceTaskMetricsListener();
    spark.sparkContext().addSparkListener(listener);
    spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "112");
    List<Row> chunked;
    try {
      chunked = spark.sql(sql).collectAsList();
      spark.sparkContext().listenerBus().waitUntilEmpty(5000);
    } finally {
      spark.sparkContext().removeSparkListener(listener);
      spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "67108864");
    }

    assertEquals(unchunked.size(), chunked.size(), "chunking must not change the row count");
    for (int row = 0; row < unchunked.size(); row++) {
      assertEquals(
          unchunked.get(row).getInt(0), chunked.get(row).getInt(0), "row " + row + " query_id");
      assertEquals(unchunked.get(row).getInt(1), chunked.get(row).getInt(1), "row " + row + " id");
      assertEquals(
          unchunked.get(row).getFloat(2),
          chunked.get(row).getFloat(2),
          1e-6f,
          "row " + row + " _distance");
    }

    // Five fragments are five units, each of which now runs two native scans instead of one; the
    // counters are summed over tasks and prove the smaller budget really took effect.
    Map<String, Long> metrics = listener.metricValues();
    assertEquals(
        Long.valueOf(10L),
        metrics.get(LanceCustomMetrics.NUM_BATCH_CHUNKS),
        "five units times two chunks, got " + metrics);
    assertEquals(
        Long.valueOf(20L),
        metrics.get(LanceCustomMetrics.NUM_BATCH_CHUNK_QUERIES),
        "five units times four queries, got " + metrics);
    assertTrue(
        metrics.getOrDefault(LanceCustomMetrics.NUM_BATCH_CHUNK_BYTES, 0L) > 0L,
        "the estimated volume must be reported, got " + metrics);
  }

  /** Each query of a chunked batch must still equal what searching that vector alone answers. */
  @Test
  void everyQueryOfAChunkedBatchEqualsItsOwnSingleSearch() {
    assumeNamedArguments();
    String table = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "112");
    try {
      List<Row> rows =
          spark
              .sql(
                  "SELECT query_id, id, _distance FROM VECTOR_SEARCH(table => '"
                      + table
                      + "', queries => array('0.0,0.0,0.0,0.0', '1.0,1.0,1.0,1.0', "
                      + "'2.0,2.0,2.0,2.0', '4.0,4.0,4.0,4.0'), k => 2)"
                      + " ORDER BY query_id, _distance, id")
              .collectAsList();
      double[] centers = {0.0, 1.0, 2.0, 4.0};
      for (int query = 0; query < centers.length; query++) {
        assertEquals(
            rankedIdsOf(singleQuerySearch(table, 2, constantVector(centers[query]))),
            idsOfQuery(rows, query),
            "query " + query + " must answer what a single search of its vector answers");
      }
    } finally {
      spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "67108864");
    }
  }

  /** A batch of one query is still a batch: one query id, and chunking has nothing to cut. */
  @Test
  void aSingleQueryBatchStillCarriesItsQueryId() {
    assumeNamedArguments();
    String table = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "1");
    try {
      List<Row> rows =
          spark
              .sql(
                  "SELECT query_id, id FROM VECTOR_SEARCH(table => '"
                      + table
                      + "', queries => array('0.0,0.0,0.0,0.0'), k => 3)"
                      + " ORDER BY query_id, _distance, id")
              .collectAsList();
      assertEquals(3, rows.size(), "one query, k rows");
      assertEquals(0, rows.get(0).getInt(0));
      assertEquals(0, rows.get(1).getInt(0));
    } finally {
      spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "67108864");
    }
  }

  /** k above the number of matching rows returns what exists, per query, without duplicating. */
  @Test
  void aChunkedBatchWithKLargerThanTheDataReturnsEachRowOnce() {
    assumeNamedArguments();
    String table = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "1");
    try {
      List<Row> rows =
          spark
              .sql(
                  "SELECT query_id, id FROM VECTOR_SEARCH(table => '"
                      + table
                      + "', queries => array('0.0,0.0,0.0,0.0', '9.0,9.0,9.0,9.0'), k => 50)"
                      + " ORDER BY query_id, _distance, id")
              .collectAsList();
      assertEquals(10, rows.size(), "two queries times the five rows that exist");
      for (int query = 0; query < 2; query++) {
        assertEquals(5, idsOfQuery(rows, query).size(), "query " + query + " sees every row once");
      }
    } finally {
      spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "67108864");
    }
  }

  /** The plan text states the division, so an EXPLAIN shows why a batch opened several scans. */
  @Test
  void theScanDescriptionNamesTheChunkDivision() {
    assumeNamedArguments();
    String table = createFiveFragmentTable();
    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "112");
    try {
      Dataset<Row> result =
          spark.sql(
              "SELECT query_id, id FROM VECTOR_SEARCH(table => '"
                  + table
                  + "', queries => array('0.0,0.0,0.0,0.0', '1.0,1.0,1.0,1.0', "
                  + "'2.0,2.0,2.0,2.0', '4.0,4.0,4.0,4.0'), k => 2)");
      assertEquals(8, result.collectAsList().size());
      String description = findSearchScan(result).description();
      assertTrue(
          description.contains("chunkQueries=2") && description.contains("chunks=2"),
          "the scan description must state the division, was: " + description);
    } finally {
      spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "67108864");
    }
  }

  private String constantVector(double value) {
    return "array(" + value + ", " + value + ", " + value + ", " + value + ")";
  }

  /** The ids one query answered, in rank order. */
  private static List<Integer> idsOfQuery(List<Row> rows, int query) {
    List<Integer> ids = new ArrayList<>();
    for (Row row : rows) {
      if (row.getInt(0) == query) {
        ids.add(row.getInt(1));
      }
    }
    return ids;
  }

  /**
   * Spark 3.4 table-valued functions have no named arguments, so the batch cases - which need
   * {@code queries => ...} - cannot even be analyzed there. Same guard as the deferred-payload
   * suite, which also builds its SQL from named arguments.
   */
  private void assumeNamedArguments() {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "Spark 3.4 table-valued functions do not support named arguments");
  }

  /** One query through the same distributed path, used as the reference for a batch member. */
  private String singleQuerySearch(String table, int k, String queryVectorSql) {
    return "SELECT id FROM VECTOR_SEARCH(table => '"
        + table
        + "', query_vector => "
        + queryVectorSql
        + ", k => "
        + k
        + ") ORDER BY _distance, id";
  }

  /** The ids a single-search SQL returns, in rank order (the SQL already orders them). */
  private List<Integer> rankedIdsOf(String sql) {
    return idsOf(sql);
  }

  private List<Integer> idsOf(String sql) {
    List<Integer> ids = new ArrayList<>();
    for (Row row : spark.sql(sql).collectAsList()) {
      ids.add(row.getInt(0));
    }
    return ids;
  }

  private static String rootMessage(Throwable throwable) {
    Throwable root = throwable;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return String.valueOf(root.getMessage());
  }
}
