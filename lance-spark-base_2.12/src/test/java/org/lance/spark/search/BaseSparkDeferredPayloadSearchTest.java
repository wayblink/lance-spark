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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.v2.LanceFetchPayloadExec;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Correctness tests for the deferred payload path of distributed VECTOR_SEARCH (card #3258): the
 * narrow search ranks candidates, Spark picks the global winners, and the payload is fetched by row
 * id afterwards.
 *
 * <p>Every test runs the same query through both plans - {@code mode=off} keeps the fused native
 * search that reads payload inside each unit, {@code mode=force} takes the deferred path - and
 * requires the two answers to be identical: same rows, same order, same row ids and distances.
 * Exact equality, not set membership, because the deferred path must not change the ranking
 * contract or the tie-breaking rule.
 */
public abstract class BaseSparkDeferredPayloadSearchTest {
  private static final String CATALOG_NAME = "lance_deferred_payload";
  private static final String MODE_CONF = "spark.sql.lance.search.payload.deferral.mode";
  private static final String CHUNK_ROWS_CONF =
      "spark.sql.lance.search.payload.deferral.chunk.rows";

  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-deferred-payload-search-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG_NAME + ".root", tempDir.toString())
            .config("spark.sql.lance.search.distributed.enabled", "true")
            .config(MODE_CONF, "off")
            .getOrCreate();
    spark.sql("CREATE NAMESPACE " + CATALOG_NAME + ".default");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  /** Spark 3.4 table-valued functions have no named arguments, so those cases cannot run there. */
  private void assumeNamedArguments() {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "Spark 3.4 table-valued functions do not support named arguments");
  }

  @Test
  void singleQueryReturnsTheSameRowsThroughBothPlans() {
    assumeNamedArguments();
    String table = createTable("single", 6);
    String sql =
        "SELECT id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 10, "
            + "columns => array('id', 'name')) ORDER BY _distance, id";

    List<Row> fused = collect(sql, "off");
    List<Row> deferred = collect(sql, "force");

    assertEquals(fused.size(), deferred.size(), "both plans must return k rows");
    assertEquals(rowsAsString(fused), rowsAsString(deferred));
  }

  @Test
  void kLargerThanTheTableReturnsEveryRowThroughBothPlans() {
    assumeNamedArguments();
    String table = createTable("klarge", 4);
    String sql =
        "SELECT id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 500, "
            + "columns => array('id', 'name')) ORDER BY _distance, id";

    List<Row> fused = collect(sql, "off");
    List<Row> deferred = collect(sql, "force");

    assertEquals(8, fused.size(), "k larger than the table returns every row");
    assertEquals(rowsAsString(fused), rowsAsString(deferred));
  }

  @Test
  void tiedDistancesKeepTheSameOrderAndRowIds() {
    assumeNamedArguments();
    String table = CATALOG_NAME + ".default.ties";
    createTableSql(table);
    // Identical vectors in four fragments: every row ties on _distance, so only the row-id
    // tie-break decides which k rows win. Both plans must agree on that order.
    for (int fragment = 0; fragment < 4; fragment++) {
      spark.sql(
          "INSERT INTO "
              + table
              + " SELECT CAST(id AS INT), array(1.0, 1.0, 1.0, 1.0), concat('n', id)"
              + " FROM range("
              + (fragment * 3)
              + ", "
              + (fragment * 3 + 3)
              + ", 1, 1)");
    }
    String sql =
        "SELECT id, name, _distance, _rowid FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 6, "
            + "columns => array('id', 'name'), with_row_id => true) ORDER BY _distance, id";

    List<Row> fused = collect(sql, "off");
    List<Row> deferred = collect(sql, "force");

    assertEquals(6, fused.size());
    assertEquals(rowsAsString(fused), rowsAsString(deferred), "tie order must not move");
  }

  @Test
  void offsetPagingAgreesBetweenBothPlans() {
    assumeNamedArguments();
    String table = createTable("paging", 5);
    String page =
        "SELECT id, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 4, offset => 4, "
            + "columns => array('id')) ORDER BY _distance, id";

    assertEquals(4, collect(page, "off").size());
    assertEquals(rowsAsString(collect(page, "off")), rowsAsString(collect(page, "force")));
  }

  @Test
  void nullablePayloadColumnsSurviveTheFetch() {
    assumeNamedArguments();
    String table = CATALOG_NAME + ".default.nullable_payload";
    createTableSql(table);
    spark.sql(
        "INSERT INTO "
            + table
            + " VALUES (0, array(0.0, 0.0, 0.0, 0.0), 'first'),"
            + " (1, array(1.0, 1.0, 1.0, 1.0), CAST(NULL AS STRING)),"
            + " (2, array(2.0, 2.0, 2.0, 2.0), 'third')");
    String sql =
        "SELECT id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 3, "
            + "columns => array('id', 'name')) ORDER BY _distance, id";

    List<Row> fused = collect(sql, "off");
    List<Row> deferred = collect(sql, "force");

    assertEquals(3, deferred.size());
    assertEquals(rowsAsString(fused), rowsAsString(deferred));
    assertEquals(true, deferred.get(1).isNullAt(1), "a null payload must stay null");
  }

  @Test
  void deletedRowsAreInvisibleToBothPlans() {
    assumeNamedArguments();
    String table = createTable("deleted", 5);
    spark.sql("DELETE FROM " + table + " WHERE id = 1");
    String sql =
        "SELECT id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 4, "
            + "columns => array('id', 'name')) ORDER BY _distance, id";

    List<Row> fused = collect(sql, "off");
    List<Row> deferred = collect(sql, "force");

    assertEquals(rowsAsString(fused), rowsAsString(deferred));
    for (Row row : deferred) {
      assertTrue(row.getInt(0) != 1, "the deleted row must not come back");
    }
  }

  /**
   * The deferred fetch must read the snapshot the search planned on, not whatever is latest when
   * the payload is fetched.
   *
   * <p>The fetch node is executed twice here on purpose. Its first call plans the scan's
   * partitions, which is where the search pins the dataset version; the second call reuses those
   * partitions (a planned partition list is fixed) and is the state a retried fetch or a second
   * action runs in. Deleting a winner in between makes a latest read silently drop its row id, so
   * the fetch fails - the search answered from one version and the payload read another. The
   * renamed payload is not used because a delete keeps row addresses valid (a deletion vector),
   * which is exactly the case the row-id verification cannot see as "a row of the wrong snapshot".
   */
  @Test
  void theFetchReadsTheVersionTheSearchPinned() {
    assumeNamedArguments();
    String table = createTable("pinned_fetch", 4);
    String sql =
        "SELECT id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 6, "
            + "columns => array('id', 'name')) ORDER BY _distance, id";

    spark.conf().set("spark.sql.adaptive.enabled", "false");
    spark.conf().set(MODE_CONF, "force");
    try {
      Dataset<Row> deferred = spark.sql(sql);
      LanceFetchPayloadExec fetch = fetchPayloadExec(deferred);

      List<String> before = payloadNames(fetch);
      assertEquals(
          java.util.Arrays.asList("n0", "n1", "n2", "n3", "n4", "n5"),
          before,
          "the first fetch reads the version the search planned on");

      // A new version with one winner deleted: its row id survives in the fragment (a deletion
      // vector, not a rewrite), and takeRows silently drops it, so a fetch at latest returns fewer
      // rows than it asked for instead of attaching the wrong payload.
      spark.sql("DELETE FROM " + table + " WHERE id = 5");
      assertEquals(
          0,
          spark.sql("SELECT id FROM " + table + " WHERE id = 5").collectAsList().size(),
          "the fixture must have committed a newer version without id 5");

      List<String> after = payloadNames(fetch);
      assertEquals(
          before, after, "the fetch read the latest version instead of the pinned snapshot");
    } finally {
      spark.conf().unset("spark.sql.adaptive.enabled");
      spark.conf().unset(MODE_CONF);
    }
  }

  /**
   * A pin that does not carry a version must fail the fetch instead of silently reading latest: the
   * scan is the only place that knows which snapshot the ranking used, so a fetch it could not pin
   * has no safe version to read. The message has to say that, or the failure looks like a storage
   * or plumbing error.
   */
  @Test
  void anUnpinnedFetchRefusesToReadTheLatestVersion() {
    assumeNamedArguments();
    String table = createTable("unpinned_fetch", 2);
    String sql =
        "SELECT id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 2, "
            + "columns => array('id', 'name')) ORDER BY _distance, id";

    spark.conf().set("spark.sql.adaptive.enabled", "false");
    spark.conf().set(MODE_CONF, "force");
    try {
      LanceFetchPayloadExec fetch = fetchPayloadExec(spark.sql(sql));
      // What a scan that failed to pin would record: the plumbing's options, no version.
      assertTrue(
          fetch.source().readOptions().getRef() == null
              || fetch.source().readOptions().getRef().getVersionNumber().isEmpty(),
          "the planner source must start unpinned for this test to mean anything");
      LanceSearchVersionPin unpinned = new LanceSearchVersionPin();
      unpinned.record(fetch.source().readOptions());

      Exception error = assertThrows(Exception.class, () -> payloadNames(withPin(fetch, unpinned)));
      assertTrue(
          rootMessage(error).contains("Refusing to read the deferred search payload"),
          rootMessage(error));
    } finally {
      spark.conf().unset("spark.sql.adaptive.enabled");
      spark.conf().unset(MODE_CONF);
    }
  }

  /**
   * A tag is not a pinned snapshot: the dataset open resolves it as a checkout of the tag's current
   * target, which can move between the ranking and the fetch. The guard must refuse it even though
   * a version number is present on the ref.
   */
  @Test
  void aMovingTagRefIsNotAPinnedSnapshotForTheFetch() {
    assumeNamedArguments();
    String table = createTable("tagged_fetch", 2);
    String sql =
        "SELECT id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 2, "
            + "columns => array('id', 'name')) ORDER BY _distance, id";

    spark.conf().set("spark.sql.adaptive.enabled", "false");
    spark.conf().set(MODE_CONF, "force");
    try {
      LanceFetchPayloadExec fetch = fetchPayloadExec(spark.sql(sql));
      LanceSearchVersionPin tagged = new LanceSearchVersionPin();
      tagged.record(
          fetch
              .source()
              .readOptions()
              .withRef(
                  new org.lance.spark.LanceRef(
                      org.lance.spark.utils.Optional.of(1L),
                      org.lance.spark.utils.Optional.<String>empty(),
                      org.lance.spark.utils.Optional.of("release"))));

      Exception error = assertThrows(Exception.class, () -> payloadNames(withPin(fetch, tagged)));
      assertTrue(
          rootMessage(error).contains("Refusing to read the deferred search payload"),
          rootMessage(error));
    } finally {
      spark.conf().unset("spark.sql.adaptive.enabled");
      spark.conf().unset(MODE_CONF);
    }
  }

  /** The same fetch node with a different pin, so a test can hand it a pin the scan would not. */
  private LanceFetchPayloadExec withPin(LanceFetchPayloadExec fetch, LanceSearchVersionPin pin) {
    return new LanceFetchPayloadExec(
        fetch.child(),
        fetch.columns(),
        fetch.fetched(),
        fetch.narrowColumns(),
        fetch.isBatch(),
        fetch.source(),
        fetch.chunkRows(),
        pin);
  }

  /**
   * The fetch reads the payload column of every winner from the fetch node's own batches. It walks
   * the physical plan to the node, because the fetch is the second phase and its RDD is only built
   * when the plan runs; executing that node directly skips Spark's shuffle reuse and re-runs the
   * fetch, which is exactly the path this test needs.
   */
  private LanceFetchPayloadExec fetchPayloadExec(Dataset<Row> query) {
    org.apache.spark.sql.execution.SparkPlan root = query.queryExecution().executedPlan();
    java.util.ArrayDeque<org.apache.spark.sql.execution.SparkPlan> pending =
        new java.util.ArrayDeque<>();
    pending.push(root);
    LanceFetchPayloadExec found = null;
    while (!pending.isEmpty()) {
      org.apache.spark.sql.execution.SparkPlan plan = pending.pop();
      if (plan instanceof LanceFetchPayloadExec) {
        found = (LanceFetchPayloadExec) plan;
      }
      scala.collection.Iterator<org.apache.spark.sql.execution.SparkPlan> children =
          plan.children().iterator();
      while (children.hasNext()) {
        pending.push(children.next());
      }
    }
    assertTrue(found != null, "the forced plan must contain a LanceFetchPayload node: " + root);
    return found;
  }

  private static List<String> payloadNames(LanceFetchPayloadExec fetch) {
    List<String> output = new ArrayList<>();
    for (org.apache.spark.sql.catalyst.expressions.Attribute column :
        scala.collection.JavaConverters.seqAsJavaList(fetch.columns())) {
      output.add(column.name());
    }
    int namePosition = output.indexOf("name");
    assertTrue(namePosition >= 0, "the fetch output must carry the payload column: " + output);

    // A dataset action calls resetMetrics() on the plan before executing it, which materializes the
    // SQL metrics on the driver. This direct execution has to do that too: the metric callback runs
    // on the executor, where the serialized plan's transient session is null, so letting the lazy
    // value initialize there would throw.
    fetch.resetMetrics();

    // The fetch RDD is columnar, and Spark's plan-level collect path only handles UnsafeRows;
    // flattening the batches to strings first keeps every task result serializable.
    ClassTag<ColumnarBatch> tag = ClassTag$.MODULE$.apply(ColumnarBatch.class);
    JavaRDD<ColumnarBatch> batches = JavaRDD.fromRDD(fetch.executeColumnar(), tag);
    return batches
        .flatMap(
            batch -> {
              List<String> names = new ArrayList<>();
              java.util.Iterator<InternalRow> rows = batch.rowIterator();
              while (rows.hasNext()) {
                names.add(rows.next().getString(namePosition));
              }
              return names.iterator();
            })
        .collect();
  }

  @Test
  void batchQueryAgreesBetweenBothPlans() {
    assumeNamedArguments();
    String table = createTable("batch", 6);
    String sql =
        "SELECT query_id, id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', queries => array('0.0,0.0,0.0,0.0', '3.0,3.0,3.0,3.0'), k => 5, "
            + "columns => array('id', 'name')) ORDER BY query_id, _distance, id";

    List<Row> fused = collect(sql, "off");
    List<Row> deferred = collect(sql, "force");

    assertEquals(10, fused.size(), "each query keeps its own k rows");
    assertEquals(rowsAsString(fused), rowsAsString(deferred));
  }

  /**
   * Chunking a batch and deferring its payload are independent cuts of the same query, so both at
   * once must still answer the unchunked fused plan's rows: the fetch attributes winners to a query
   * by the same `query_index` the chunk base shifted.
   */
  @Test
  void aChunkedDeferredBatchAgreesWithTheUnchunkedFusedPlan() throws Exception {
    assumeNamedArguments();
    String table = createTable("chunked_batch", 6);
    String sql =
        "SELECT query_id, id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', queries => array('0.0,0.0,0.0,0.0', '3.0,3.0,3.0,3.0', "
            + "'6.0,6.0,6.0,6.0'), k => 3, columns => array('id', 'name'))"
            + " ORDER BY query_id, _distance, id";

    List<Row> fused = collect(sql, "off");
    assertEquals(9, fused.size(), "three queries times k rows");

    // 64 bytes admits one query per native scan in both shapes: the fused scan charges the payload
    // width for `id` and `name`, the deferred narrow scan does not, and both stay above 64 bytes.
    LanceTaskMetricsListener listener = new LanceTaskMetricsListener();
    spark.sparkContext().addSparkListener(listener);
    spark.conf().set("spark.sql.lance.search.batch.chunk.bytes", "64");
    try {
      List<Row> chunkedFused = collect(sql, "off");
      List<Row> chunkedDeferred = collect(sql, "force");
      spark.sparkContext().listenerBus().waitUntilEmpty(5000);
      assertEquals(rowsAsString(fused), rowsAsString(chunkedFused), "chunking alone");
      assertEquals(
          rowsAsString(fused),
          rowsAsString(chunkedDeferred),
          "chunking plus a deferred fetch must attribute every payload to its own query");
      // Six fragments are six units and each chunk holds one query, so the runs really were cut.
      java.util.Map<String, Long> metrics = listener.metricValues();
      assertTrue(
          metrics.getOrDefault(org.lance.spark.read.metric.LanceCustomMetrics.NUM_BATCH_CHUNKS, 0L)
              >= 18L,
          "both runs must have chunked: six units times three chunks, got " + metrics);
    } finally {
      spark.sparkContext().removeSparkListener(listener);
      spark.conf().unset("spark.sql.lance.search.batch.chunk.bytes");
    }
  }

  @Test
  void smallFetchChunksReturnTheSameRows() {
    assumeNamedArguments();
    String table = createTable("chunks", 6);
    String sql =
        "SELECT id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 12, "
            + "columns => array('id', 'name')) ORDER BY _distance, id";

    List<Row> fused = collect(sql, "off");
    spark.conf().set(CHUNK_ROWS_CONF, "2");
    try {
      List<Row> deferred = collect(sql, "force");
      assertEquals(rowsAsString(fused), rowsAsString(deferred), "2-row chunks must not reorder");
    } finally {
      spark.conf().unset(CHUNK_ROWS_CONF);
    }
  }

  @Test
  void emptyTableReturnsNoRowsThroughBothPlans() {
    assumeNamedArguments();
    String table = CATALOG_NAME + ".default.empty_payload";
    createTableSql(table);
    String sql =
        "SELECT id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 5, "
            + "columns => array('id', 'name'))";

    assertEquals(0, collect(sql, "off").size());
    assertEquals(0, collect(sql, "force").size());
  }

  @Test
  void forcedDeferralShowsUpInThePlanAndOffKeepsTheFusedPlan() {
    assumeNamedArguments();
    String table = createTable("plan", 3);
    String sql =
        "SELECT id, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 2, columns => array('id'))";

    spark.conf().set("spark.sql.adaptive.enabled", "false");
    try {
      assertFalse(planOf(sql, "off").contains("LanceFetchPayload"), planOf(sql, "off"));
      String deferredPlan = planOf(sql, "force");
      assertTrue(deferredPlan.contains("LanceFetchPayload"), deferredPlan);
      // The native search below the ranking must not ask for the payload column.
      assertFalse(
          deferredPlan.contains("columns: [id, _distance]"),
          "a deferred plan still read the payload column natively: " + deferredPlan);
    } finally {
      spark.conf().unset("spark.sql.adaptive.enabled");
    }
  }

  @Test
  void positionalSearchWithoutColumnsFetchesEveryDeclaredPayloadColumn() {
    // No `columns` argument: the deferred path has to fetch the whole declared row, vector column
    // included. That is the shape the frozen benchmark queries use, and it is the one where a
    // request can get large. Positional form so this also runs on Spark 3.4.
    String table = createTable("no_columns", 5);
    String sql =
        "SELECT * FROM VECTOR_SEARCH('"
            + table
            + "', array(0.0, 0.0, 0.0, 0.0), 4) ORDER BY _distance, id";

    List<Row> fused = collect(sql, "off");
    List<Row> deferred = collect(sql, "force");

    assertEquals(4, fused.size());
    assertEquals(rowsAsString(fused), rowsAsString(deferred));
    for (Row row : deferred) {
      assertEquals(4, row.size(), "id, vector, name and _distance");
      assertEquals(4, row.getList(1).size(), "the vector payload must survive the fetch");
    }
  }

  @Test
  void aRealQueryIndexColumnIsFetchedNotClobbered() {
    assumeNamedArguments();
    // `query_index` is only the batch's internal column. In a single-query search a real column of
    // that name is payload, and it must come back with its stored values instead of the 0 the
    // internal column would contribute.
    String table = CATALOG_NAME + ".default.query_index_column";
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id INT NOT NULL, query_index INT NOT NULL, vector ARRAY<FLOAT> NOT NULL) "
            + "USING lance TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    for (int fragment = 0; fragment < 4; fragment++) {
      int start = fragment * 2;
      spark.sql(
          "INSERT INTO "
              + table
              + " SELECT CAST(id AS INT), CAST(id + 1000 AS INT),"
              + " array(CAST(id AS FLOAT), CAST(id AS FLOAT), CAST(id AS FLOAT),"
              + " CAST(id AS FLOAT)) FROM range("
              + start
              + ", "
              + (start + 2)
              + ", 1, 1)");
    }
    String sql =
        "SELECT id, query_index, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 4, "
            + "columns => array('id', 'query_index')) ORDER BY _distance, id";

    List<Row> fused = collect(sql, "off");
    List<Row> deferred = collect(sql, "force");

    assertEquals(rowsAsString(fused), rowsAsString(deferred));
    for (Row row : deferred) {
      assertEquals(
          row.getInt(0) + 1000, row.getInt(1), "the real query_index value must be fetched");
    }
  }

  @Test
  void notNullPayloadColumnsStayNotNullable() {
    assumeNamedArguments();
    String table = createTable("nullability", 2);
    String sql =
        "SELECT id FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 2, columns => array('id'))";

    spark.conf().set(MODE_CONF, "off");
    boolean fusedNullable = spark.sql(sql).schema().apply("id").nullable();
    spark.conf().set(MODE_CONF, "force");
    boolean deferredNullable = spark.sql(sql).schema().apply("id").nullable();

    assertFalse(fusedNullable, "the fixture column is NOT NULL");
    assertEquals(
        fusedNullable,
        deferredNullable,
        "a deferred fetch must not report a NOT NULL payload column as nullable");
  }

  @Test
  void forcedDeferralRejectsAPayloadColumnItCannotFetchByRowId() {
    assumeNamedArguments();
    String table = CATALOG_NAME + ".default.nested_payload";
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id INT NOT NULL, meta STRUCT<a: INT>, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
    spark.sql(
        "INSERT INTO "
            + table
            + " SELECT CAST(id AS INT), named_struct('a', CAST(id AS INT)),"
            + " array(CAST(id AS FLOAT), CAST(id AS FLOAT), CAST(id AS FLOAT),"
            + " CAST(id AS FLOAT)) FROM range(0, 4, 1, 2)");
    String sql =
        "SELECT id, meta, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 2, "
            + "columns => array('id', 'meta'))";

    // A nested column does not round-trip through takeRows with the scan layout, so `force` says so
    // instead of quietly running the fused plan the caller asked to avoid.
    spark.conf().set(MODE_CONF, "force");
    Exception error = assertThrows(Exception.class, () -> spark.sql(sql).collectAsList());
    assertTrue(rootMessage(error).contains("cannot fetch payload column"), rootMessage(error));

    // `auto` is allowed to fall back, and the fused plan still answers.
    spark.conf().set(MODE_CONF, "auto");
    assertEquals(2, spark.sql(sql).collectAsList().size());
  }

  private static String rootMessage(Throwable error) {
    Throwable current = error;
    while (current.getCause() != null) {
      current = current.getCause();
    }
    return String.valueOf(current.getMessage());
  }

  @Test
  void explainDoesNotExposeCredentialsOrObjectIdentity() {
    assumeNamedArguments();
    String table = createTable("explain", 2);
    String sql =
        "SELECT id, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 2, columns => array('id'))";
    spark.conf().set(MODE_CONF, "force");

    StringBuilder plan = new StringBuilder();
    for (Row row : spark.sql("EXPLAIN EXTENDED " + sql).collectAsList()) {
      plan.append(row.getString(0)).append('\n');
    }
    String text = plan.toString();

    assertTrue(text.contains("LanceFetchPayload([id]"), "the node must be visible in the plan");
    assertFalse(text.contains("LanceSparkReadOptions"), "plan text leaked read options: " + text);
    assertFalse(
        text.contains("LanceFetchPayloadSource"),
        "plan text leaked the credential holder: " + text);
    assertFalse(text.contains("credentials=redacted"), "plan text printed the holder: " + text);
    assertFalse(
        java.util.regex.Pattern.compile("@[0-9a-f]{6,}").matcher(text).find(),
        "plan text printed an object identity hash: " + text);
  }

  @Test
  void theCredentialHolderNeverPrintsItsStorageOptions() {
    java.util.Map<String, String> storage =
        new java.util.HashMap<>(
            java.util.Collections.singletonMap("aws_secret_access_key", "SENTINEL3258"));
    LanceFetchPayloadSource source =
        new LanceFetchPayloadSource(
            org.lance.spark.LanceSparkReadOptions.from("file:///tmp/x.lance"),
            storage,
            "dir",
            java.util.Collections.singletonMap("root", "/tmp"),
            org.lance.spark.internal.DatasetLeaseSettings.defaults());

    assertFalse(source.toString().contains("SENTINEL3258"), source.toString());
    assertFalse(source.toString().contains("aws_secret_access_key"), source.toString());
    assertTrue(source.toString().contains("redacted"), source.toString());
  }

  @Test
  void deferredFetchRunsInsideAnAdaptiveExecution() {
    assumeNamedArguments();
    String table = createTable("aqe", 4);
    String sql =
        "SELECT id, name, _distance FROM VECTOR_SEARCH(table => '"
            + table
            + "', query_vector => array(0.0, 0.0, 0.0, 0.0), k => 6, "
            + "columns => array('id', 'name')) ORDER BY _distance, id";

    spark.conf().set("spark.sql.adaptive.enabled", "true");
    spark.conf().set(MODE_CONF, "force");
    try {
      org.apache.spark.sql.Dataset<Row> deferred = spark.sql(sql);
      // The fetch collects its winners from inside the action and starts a second job there; with
      // AQE the plan is wrapped, so this asserts the shape the nested job actually runs under.
      assertTrue(
          deferred.queryExecution().executedPlan()
              instanceof org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec,
          deferred.queryExecution().executedPlan().toString());
      List<Row> rows = new ArrayList<>(deferred.collectAsList());
      assertEquals(6, rows.size());
      spark.conf().set(MODE_CONF, "off");
      assertEquals(
          rowsAsString(rows), rowsAsString(new ArrayList<>(spark.sql(sql).collectAsList())));
    } finally {
      spark.conf().set("spark.sql.adaptive.enabled", "true");
    }
  }

  private List<Row> collect(String sql, String mode) {
    spark.conf().set(MODE_CONF, mode);
    return new ArrayList<>(spark.sql(sql).collectAsList());
  }

  private String planOf(String sql, String mode) {
    spark.conf().set(MODE_CONF, mode);
    Dataset<Row> df = spark.sql(sql);
    return df.queryExecution().executedPlan().toString();
  }

  private static List<String> rowsAsString(List<Row> rows) {
    List<String> rendered = new ArrayList<>(rows.size());
    for (Row row : rows) {
      rendered.add(row.mkString("|"));
    }
    return rendered;
  }

  /**
   * Six fragments of two rows: enough searchable units that the per-unit payload copies the
   * deferred path removes are real, and small enough to stay a second-scale test.
   */
  private String createTable(String suffix, int fragments) {
    String fullName = CATALOG_NAME + ".default." + suffix;
    createTableSql(fullName);
    for (int fragment = 0; fragment < fragments; fragment++) {
      int start = fragment * 2;
      spark.sql(
          "INSERT INTO "
              + fullName
              + " SELECT CAST(id AS INT), array(CAST(id AS FLOAT), CAST(id AS FLOAT),"
              + " CAST(id AS FLOAT), CAST(id AS FLOAT)), concat('n', id)"
              + " FROM range("
              + start
              + ", "
              + (start + 2)
              + ", 1, 1)");
    }
    return fullName;
  }

  private void createTableSql(String fullName) {
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL, name STRING) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '4')");
  }
}
