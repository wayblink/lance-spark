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
package org.lance.spark.read;

import org.lance.namespace.DirectoryNamespace;
import org.lance.namespace.model.QueryTableRequest;
import org.lance.namespace.model.QueryTableResponse;
import org.lance.spark.LanceSparkReadOptions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Row-for-row differential test for the strict-equivalence filter whitelist of card #3263.
 *
 * <p>Each SQL predicate runs twice over the same Lance dataset: once with filter pushdown enabled
 * and once with {@code pushDownFilters=false}, which leaves the whole predicate as a Spark residual
 * filter. The two result sets must be identical. For every predicate the whitelist claims to
 * translate, the pushed plan is also required to show the rendered SQL in its scan metadata, so a
 * silently non-pushed case cannot pass as "equivalent".
 *
 * <p>The fixture deliberately mixes nulls, the empty string, negative numbers, decimals, quotes,
 * LIKE metacharacters and non-ASCII case-folding corners ({@code ß}, {@code İ}, a ligature, an
 * emoji). Those are the values where a naive rewrite (upper-casing, LIKE without escaping, integer
 * division) diverges from Spark.
 */
public abstract class BaseFilterPushDownEquivalenceTest {

  private static final String RECORDING_CATALOG = "filter_equiv_ns";
  private static final String FTS_INDEX_OPTIONS =
      "base_tokenizer='simple', language='English', max_token_length=40, lower_case=true, "
          + "stem=false, remove_stop_words=false, ascii_folding=false, with_position=true";

  private static final List<String> STRINGS =
      Arrays.asList(
          null,
          "",
          "abc",
          "ABC",
          "Straße",
          "ß",
          "İstanbul",
          "ıI",
          "a%b",
          "a_b",
          "a\\b",
          "😀emoji",
          "  spaced  ",
          "ﬁle",
          "O'Brien");

  protected SparkSession spark;

  @TempDir Path tempDir;

  private Path dataPath;
  private Path namespaceRoot;

  @BeforeEach
  public void setup() throws Exception {
    namespaceRoot = tempDir.resolve("nsroot");
    dataPath = tempDir.resolve("data");
    Files.createDirectories(namespaceRoot);

    spark =
        SparkSession.builder()
            .appName("lance-filter-pushdown-equivalence-test")
            .master("local[4]")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config(
                "spark.sql.catalog." + RECORDING_CATALOG,
                "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.catalog." + RECORDING_CATALOG + ".impl",
                RecordingDirectoryNamespace.class.getName())
            .config("spark.sql.catalog." + RECORDING_CATALOG + ".root", namespaceRoot.toString())
            .config("spark.sql.catalog." + RECORDING_CATALOG + ".single_level_ns", "true")
            .config("spark.sql.defaultCatalog", RECORDING_CATALOG)
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    writeFixture();
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  private void writeFixture() {
    List<Row> rows = new ArrayList<>();
    for (int id = 0; id < STRINGS.size(); id++) {
      long big = id % 3 == 0 ? -5L : (id % 3 == 1 ? 0L : 7L);
      BigDecimal dec = id % 4 == 0 ? null : new BigDecimal(id % 2 == 0 ? "1.10" : "-2.50");
      Double dbl = id % 4 == 0 ? null : (id % 2 == 0 ? -1.5d : 2.25d);
      rows.add(RowFactory.create(id, STRINGS.get(id), big, dec, dbl));
    }
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("s", DataTypes.StringType, true)
            .add("big", DataTypes.LongType, true)
            .add("dec", DataTypes.createDecimalType(10, 2), true)
            .add("dbl", DataTypes.DoubleType, true);
    spark
        .createDataFrame(rows, schema)
        .write()
        .format("lance")
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, dataPath.toString())
        .save();
  }

  private Dataset<Row> read(boolean pushdown) {
    org.apache.spark.sql.DataFrameReader reader =
        spark
            .read()
            .format("lance")
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, dataPath.toString());
    if (!pushdown) {
      reader = reader.option(LanceSparkReadOptions.CONFIG_PUSH_DOWN_FILTERS, "false");
    }
    return reader.load();
  }

  // ────────────────────────────────────────────────────────────────────────────
  // String predicates: STARTS_WITH / ENDS_WITH / CONTAINS and LIKE boundaries
  // ────────────────────────────────────────────────────────────────────────────

  @Test
  public void testStringPredicatesAreEquivalent() {
    assertEquivalent("startsWith(s, 'a')", "LIKE 'a%' ESCAPE '\\'", true);
    assertEquivalent("endsWith(s, 'c')", "LIKE '%c' ESCAPE '\\'", true);
    assertEquivalent("contains(s, 'b')", "LIKE '%b%' ESCAPE '\\'", true);

    // Needles containing LIKE metacharacters must match them literally.
    assertEquivalent("contains(s, 'a%b')", "LIKE '%a\\%b%' ESCAPE '\\'", true);
    assertEquivalent("startsWith(s, 'a%')", "LIKE 'a\\%%' ESCAPE '\\'", true);
    assertEquivalent("endsWith(s, '_b')", "LIKE '%\\_b' ESCAPE '\\'", true);
    // Spark parses string literals with backslash escapes by default (escapedStringLiterals=false):
    // the SQL below is equivalent to contains(s, "O'B"), and the rendered LIKE must double the
    // quote for Lance's SQL parser instead.
    assertEquivalent("contains(s, 'O\\'B')", "LIKE '%O''B%' ESCAPE '\\'", true);
    // A literal backslash in the needle must be doubled for Lance's LIKE escape.
    // (Spark's default parser reads ``\\`` as one backslash; a single ``\`` would be a control
    // escape and never reach the connector as data.)
    assertEquivalent("contains(s, 'a\\\\b')", "LIKE '%a\\\\b%' ESCAPE '\\'", true);

    // Spark's LikeSimplification turns these simple patterns into the same string predicates.
    assertEquivalent("s LIKE 'ab%'", "LIKE 'ab%' ESCAPE '\\'", true);
    assertEquivalent("s LIKE '%bc'", "LIKE '%bc' ESCAPE '\\'", true);
    assertEquivalent("s LIKE '%b%'", "LIKE '%b%' ESCAPE '\\'", true);

    // A pattern that cannot be simplified stays a Spark residual; the whitelist must not claim it.
    assertEquivalent("s LIKE 'a%b%c'", "LIKE 'a%b%c'", false);
    assertEquivalent("s LIKE '%a_b%'", "LIKE '%a_b%'", false);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Scalar functions, <> and the existing comparison/IN/null arms
  // ────────────────────────────────────────────────────────────────────────────

  @Test
  public void testScalarFunctionsAndComparisonsAreEquivalent() {
    assertEquivalent("upper(s) = 'ABC'", "upper(s) == 'ABC'", true);
    assertEquivalent("lower(s) = 'abc'", "lower(s) == 'abc'", true);
    assertEquivalent("length(s) = 3", "char_length(s) == 3", true);
    assertEquivalent("length(s) = 1", "char_length(s) == 1", true);

    // Unicode case-folding corners: only equivalent if both engines expand/map identically.
    assertEquivalent("upper(s) = 'STRASSE'", "upper(s) == 'STRASSE'", true);
    assertEquivalent("lower(s) = '\u0069\u0307stanbul'", "lower(s) == 'i\u0307stanbul'", true);

    // Spark hands this to the connector as NOT(id = 3) rather than as a "<>" predicate.
    assertEquivalent("id <> 3", "NOT (id == 3)", true);
    assertEquivalent("big IN (0, 7)", "big IN (0,7)", true);
    assertEquivalent("s IS NULL", "s IS NULL", true);
    assertEquivalent("s IS NOT NULL", "s IS NOT NULL", true);
    assertEquivalent("s = ''", "s == ''", true);
    assertEquivalent("dec > 1.0", null, true);
    assertEquivalent("dbl < 0.0", null, true);
    assertEquivalent("lower(s) = 'abc' AND big IN (-5, 0)", "lower(s) == 'abc'", true);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Arithmetic: pushed only when Spark runs in ANSI mode
  // ────────────────────────────────────────────────────────────────────────────

  @Test
  public void testArithmeticPushdownIsEquivalentUnderAnsi() {
    spark.conf().set("spark.sql.ansi.enabled", "true");
    try {
      assertEquivalent("big % 2 = 1", "(big % 2) == 1", true);
      assertEquivalent("big + 1 = 0", "(big + 1) == 0", true);
      assertEquivalent("big * 2 = -10", "(big * 2) == -10", true);
      assertEquivalent("id - 1 >= 2", "(id - 1) >= 2", true);
      assertEquivalent("big % 2 = 1 AND lower(s) = 'abc'", "(big % 2) == 1", true);

      // Division is not equivalent (Spark returns a double, the plan keeps integral division), so
      // it must stay residual even though Spark offers the expression to the connector.
      assertEquivalent("big / 2 = 0", "big / 2", false);
      // Unknown functions stay residual too.
      assertEquivalent("abs(big) = 5", "abs(", false);
    } finally {
      spark.conf().set("spark.sql.ansi.enabled", "false");
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Residual reduction on the namespace FTS path: pushed filters bound the server-side k
  // ────────────────────────────────────────────────────────────────────────────

  @Test
  public void testFtsResidualReductionBoundsServerSideK() {
    spark.conf().set("spark.sql.ansi.enabled", "true");
    try {
      String full = RECORDING_CATALOG + ".default.fts_bounded";
      spark.sql("CREATE TABLE " + full + " (id INT, body STRING) USING lance");
      StringBuilder values = new StringBuilder();
      for (int i = 0; i < 20; i++) {
        if (values.length() > 0) {
          values.append(", ");
        }
        values.append('(').append(i).append(", '");
        for (int t = 0; t <= i; t++) {
          if (t > 0) {
            values.append(' ');
          }
          values.append("alpha");
        }
        values.append("')");
      }
      spark.sql("INSERT INTO " + full + " VALUES " + values);
      spark.sql(
          "ALTER TABLE "
              + full
              + " CREATE INDEX fts_body USING fts (body) WITH ("
              + FTS_INDEX_OPTIONS
              + ")");

      // `big % 2` is now pushable: no residual predicate remains, so the connector sends the real
      // LIMIT as `k` instead of Integer.MAX_VALUE.
      RecordingDirectoryNamespace.reset();
      List<Integer> pushedIds =
          spark
              .sql(
                  "SELECT id FROM "
                      + full
                      + " WHERE lance_match(body, 'alpha') AND id % 2 = 1 LIMIT 5")
              .collectAsList()
              .stream()
              .map(row -> row.getInt(0))
              .collect(Collectors.toList());
      QueryTableRequest pushedRequest = lastRequest();
      assertEquals(
          Integer.valueOf(5), pushedRequest.getK(), "a fully pushed filter must bound the FTS k");
      assertEquals(Arrays.asList(19, 17, 15, 13, 11), pushedIds);
      assertTrue(
          pushedRequest.getFilter() != null && pushedRequest.getFilter().contains("(id % 2)"),
          "the pushed filter must reach queryTable; got " + pushedRequest.getFilter());
      System.out.println(
          "FTS| pushed k="
              + pushedRequest.getK()
              + " filter="
              + pushedRequest.getFilter()
              + " ids="
              + pushedIds);

      // `_score > 0` is always residual: the request must fall back to k=Integer.MAX_VALUE and
      // Spark keeps the global Filter/TopN.
      RecordingDirectoryNamespace.reset();
      List<Integer> residualIds =
          spark
              .sql(
                  "SELECT id FROM "
                      + full
                      + " WHERE lance_match(body, 'alpha') AND _score > 0 LIMIT 5")
              .collectAsList()
              .stream()
              .map(row -> row.getInt(0))
              .collect(Collectors.toList());
      QueryTableRequest residualRequest = lastRequest();
      assertEquals(
          Integer.valueOf(Integer.MAX_VALUE),
          residualRequest.getK(),
          "a residual predicate must keep the unbounded server-side k");
      assertEquals(5, residualIds.size());
      assertEquals(
          Arrays.asList(19, 18, 17, 16, 15),
          residualIds,
          "the residual path is still correct through Spark's own TopN");
      assertNull(residualRequest.getFilter(), "no structured predicate should be pushed here");
      System.out.println("FTS| residual k=" + residualRequest.getK() + " ids=" + residualIds);
    } finally {
      spark.conf().set("spark.sql.ansi.enabled", "false");
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // helpers
  // ────────────────────────────────────────────────────────────────────────────

  private void assertEquivalent(String predicate, String expectedPushedFragment, boolean pushed) {
    Dataset<Row> pushedRows = read(true).where(predicate).select("id").orderBy("id");
    Dataset<Row> residualRows = read(false).where(predicate).select("id").orderBy("id");

    List<Integer> expected = ids(residualRows);
    assertEquals(
        expected,
        ids(pushedRows),
        "pushdown changed the result of: "
            + predicate
            + " (expected residual ids "
            + expected
            + ")");

    Map<String, String> meta = LanceScanTestHelper.extractLanceScanMetadata(pushedRows);
    String where = meta == null ? null : meta.get("whereConditions");
    boolean empty = where == null || where.equals("Optional.empty");
    if (pushed) {
      assertFalse(empty, "expected a pushed filter for: " + predicate + " but the scan had none");
      if (expectedPushedFragment != null) {
        assertTrue(
            where.contains(expectedPushedFragment),
            "expected pushed SQL to contain <"
                + expectedPushedFragment
                + "> for: "
                + predicate
                + " but got "
                + where);
      }
    } else {
      assertFalse(
          where != null && where.contains(expectedPushedFragment),
          "expected the predicate to stay residual for: "
              + predicate
              + " but the scan pushed "
              + where);
      assertTrue(
          pushedRows.queryExecution().executedPlan().toString().contains("Filter "),
          "a residual predicate must leave a Spark Filter above the scan: " + predicate);
    }
    System.out.println(
        String.format(
            Locale.ROOT,
            "EQUIV| %-45s pushed=%-5s rows=%-2d where=%s",
            predicate,
            pushed,
            expected.size(),
            empty ? "Optional.empty" : where));
  }

  private static List<Integer> ids(Dataset<Row> dataset) {
    return dataset.collectAsList().stream().map(row -> row.getInt(0)).collect(Collectors.toList());
  }

  private static QueryTableRequest lastRequest() {
    List<QueryTableRequest> requests = RecordingDirectoryNamespace.requests();
    assertFalse(requests.isEmpty(), "the namespace FTS path did not call queryTable");
    QueryTableRequest request = requests.get(requests.size() - 1);
    assertNotNull(request.getFullTextQuery(), "the observed request must be the FTS request");
    return request;
  }

  /** Records every queryTable request so the FTS test can inspect the pushed k and filter. */
  public static class RecordingDirectoryNamespace extends DirectoryNamespace {
    private static final List<QueryTableRequest> REQUESTS = new CopyOnWriteArrayList<>();

    static void reset() {
      REQUESTS.clear();
    }

    static List<QueryTableRequest> requests() {
      return REQUESTS;
    }

    @Override
    public QueryTableResponse queryTable(QueryTableRequest request) {
      REQUESTS.add(request);
      return super.queryTable(request);
    }
  }
}
