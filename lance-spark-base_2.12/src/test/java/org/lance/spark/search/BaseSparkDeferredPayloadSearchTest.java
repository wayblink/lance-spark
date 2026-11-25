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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

  @Test
  void singleQueryReturnsTheSameRowsThroughBothPlans() {
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

  @Test
  void batchQueryAgreesBetweenBothPlans() {
    Assumptions.assumeFalse(
        spark.version().startsWith("3.4."),
        "Spark 3.4 table-valued functions do not support named arguments");
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

  @Test
  void smallFetchChunksReturnTheSameRows() {
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
