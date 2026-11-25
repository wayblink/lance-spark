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

import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.namespace.DirectoryNamespace;
import org.lance.namespace.model.QueryTableRequest;
import org.lance.namespace.model.QueryTableRequestColumns;
import org.lance.namespace.model.QueryTableRequestVector;
import org.lance.namespace.model.QueryTableResponse;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRuntime;
import org.lance.spark.utils.Utils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Local probe for U5 of card #3263: can a rowid range bound a ranked FTS read, and is {@code
 * prefilter=true} strictly "filter before ranking"?
 *
 * <ul>
 *   <li>U5-1: is {@code _rowid} accepted as a {@code queryTable} filter column, and is the filter
 *       really applied (not silently ignored)?
 *   <li>U5-2: does {@code prefilter=true} rank only the filtered rows (so a bounded rowid range
 *       gives the exact top-k of that range), while {@code prefilter=false} ranks first and filters
 *       afterwards?
 *   <li>U5-3: does {@code k=0} still mean "no limit" (with and without a filter)?
 *   <li>U5-4: do rowids survive an append and a partially-covered FTS index?
 * </ul>
 *
 * <p>This is a probe, not a shipped contract: every assertion pins the behaviour observed with
 * lance-core 14.0.3 / lance-namespace 0.12.0 so a rerun detects drift. It is intentionally not part
 * of the production test suite's guarantees.
 */
public abstract class BaseFtsU5ProbeTest {

  private static final String NS_CATALOG = "u5_fts_ns";
  private static final String FTS_INDEX_OPTIONS =
      "base_tokenizer='simple', language='English', max_token_length=40, lower_case=true, "
          + "stem=false, remove_stop_words=false, ascii_folding=false, with_position=true";

  private static SparkSession spark;
  private static Path namespaceRoot;

  @TempDir static Path tempDir;

  @BeforeAll
  static void startSpark() throws IOException {
    namespaceRoot = tempDir.resolve("nsroot");
    Files.createDirectories(namespaceRoot);
    spark =
        SparkSession.builder()
            .appName("lance-fts-u5-probe")
            .master("local[4]")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.defaultCatalog", NS_CATALOG)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.catalog." + NS_CATALOG, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.catalog." + NS_CATALOG + ".impl",
                RecordingDirectoryNamespace.class.getName())
            .config("spark.sql.catalog." + NS_CATALOG + ".root", namespaceRoot.toString())
            .config("spark.sql.catalog." + NS_CATALOG + ".single_level_ns", "true")
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // U5-1 — _rowid as a filter column
  // ────────────────────────────────────────────────────────────────────────────

  @Test
  public void u5RowIdIsAnApplicableFilterColumn() throws Exception {
    RecordingDirectoryNamespace.reset();
    List<String> bodies = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      bodies.add(i % 4 == 0 ? "alpha" : (i % 4 == 1 ? "alpha beta" : "gamma alpha"));
    }
    String table = createFtsTable("u5_rowid_filter", bodies, 2);
    QueryTableRequest template = captureSparkRequest(table);
    DirectoryNamespace direct = newDirectNamespace();
    probe("U5-1 table=%s fragments=%d", table, fragmentCount(table));

    List<Map<String, Object>> full =
        query(direct, template, Integer.MAX_VALUE, null, true, null, true);
    printArrowSchema(direct, template);
    probe("U5-1 full rows=%d", full.size());

    Map<Long, Integer> rowIdToId = new LinkedHashMap<>();
    for (Map<String, Object> row : full) {
      Object raw = row.get(LanceConstant.ROW_ID);
      assertNotNull(raw, "_rowid must be present when withRowId=true; row keys=" + row.keySet());
      long rowId = asLong(raw);
      assertFalse(
          rowIdToId.containsKey(rowId),
          "duplicate rowid " + rowId + " (raw=" + raw + ", " + raw.getClass() + ")");
      rowIdToId.put(rowId, ((Number) row.get("id")).intValue());
    }
    probe(
        "U5-1 rowid rawType=%s min=%d max=%d distinct=%d",
        full.get(0).get(LanceConstant.ROW_ID).getClass().getName(),
        Collections.min(rowIdToId.keySet()),
        Collections.max(rowIdToId.keySet()),
        rowIdToId.size());

    // A contiguous slice of the rowid space: this is the shape B' would request.
    List<Long> sortedRowIds = new ArrayList<>(new TreeSet<>(rowIdToId.keySet()));
    List<Long> slice = sortedRowIds.subList(0, Math.max(1, sortedRowIds.size() / 2));
    long lo = slice.get(0);
    long hi = slice.get(slice.size() - 1) + 1;
    Set<Integer> expected =
        rowIdToId.entrySet().stream()
            .filter(e -> e.getKey() >= lo && e.getKey() < hi)
            .map(Map.Entry::getValue)
            .collect(Collectors.toCollection(TreeSet::new));
    String rangeFilter = "_rowid >= " + lo + " AND _rowid < " + hi;
    List<Map<String, Object>> ranged =
        query(direct, template, Integer.MAX_VALUE, null, true, rangeFilter, true);
    assertEquals(
        expected,
        sortedIds(ranged),
        "range filter on _rowid must return exactly the rows whose rowid is in [lo, hi); filter="
            + rangeFilter);
    probe("U5-1 range filter='%s' rows=%d ids=%s", rangeFilter, ranged.size(), sortedIds(ranged));

    // Equality / IN list (the native take path) on the same column.
    List<Long> inList = slice.subList(0, Math.min(3, slice.size()));
    String inFilter =
        "_rowid IN ("
            + inList.stream().map(String::valueOf).collect(Collectors.joining(", "))
            + ")";
    Set<Integer> expectedIn =
        inList.stream().map(rowIdToId::get).collect(Collectors.toCollection(TreeSet::new));
    List<Map<String, Object>> inRows =
        query(direct, template, Integer.MAX_VALUE, null, true, inFilter, true);
    assertEquals(
        expectedIn, sortedIds(inRows), "IN filter on _rowid must return exactly those rows");
    probe("U5-1 in filter='%s' rows=%d ids=%s", inFilter, inRows.size(), sortedIds(inRows));

    // A range that cannot contain any row must return nothing: proves the filter is applied, not
    // ignored (an ignored filter would return the full match set).
    long emptyLo = Collections.max(rowIdToId.keySet()) + 1024;
    String emptyFilter = "_rowid >= " + emptyLo;
    List<Map<String, Object>> empty =
        query(direct, template, Integer.MAX_VALUE, null, true, emptyFilter, true);
    assertEquals(
        Collections.emptySet(),
        sortedIds(empty),
        "a rowid range beyond every row must return nothing; filter=" + emptyFilter);
    probe("U5-1 empty filter='%s' rows=%d", emptyFilter, empty.size());

    // Also legal with prefilter=false (postfilter): the filter must still be evaluated.
    List<Map<String, Object>> postfiltered =
        query(direct, template, Integer.MAX_VALUE, null, true, rangeFilter, false);
    assertEquals(
        expected,
        sortedIds(postfiltered),
        "prefilter=false must still apply the filter to the ranked output");
    probe("U5-1 prefilter=false range rows=%d", postfiltered.size());
  }

  // ────────────────────────────────────────────────────────────────────────────
  // U5-2 — prefilter=true is filter-before-rank
  // ────────────────────────────────────────────────────────────────────────────

  @Test
  public void u5PrefilterRanksFilteredRowsOnly() throws Exception {
    RecordingDirectoryNamespace.reset();
    // Two clearly separated score bands: ids 0..19 have tf 1..20, ids 20..39 have tf 21..40.
    List<String> bodies = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      bodies.add(repeat("alpha", i + 1));
    }
    String table = createFtsTable("u5_prefilter", bodies, 2);
    QueryTableRequest template = captureSparkRequest(table);
    DirectoryNamespace direct = newDirectNamespace();

    // Ground truth: rank everything, filter in Java, keep the first 5.
    List<Map<String, Object>> full =
        query(direct, template, Integer.MAX_VALUE, null, true, null, true);
    List<Map<String, Object>> lowBandFull =
        full.stream()
            .filter(r -> ((Number) r.get("id")).intValue() < 20)
            .collect(Collectors.toList());
    List<Integer> expectedIds =
        lowBandFull.stream()
            .limit(5)
            .map(r -> ((Number) r.get("id")).intValue())
            .collect(Collectors.toList());
    List<Float> expectedScores =
        lowBandFull.stream()
            .limit(5)
            .map(r -> ((Number) r.get(LanceConstant.SCORE)).floatValue())
            .collect(Collectors.toList());
    assertEquals(5, expectedIds.size(), "fixture must have at least 5 rows in the low band");
    List<Integer> globalTop5 =
        full.stream()
            .limit(5)
            .map(r -> ((Number) r.get("id")).intValue())
            .collect(Collectors.toList());
    assertTrue(
        globalTop5.stream().allMatch(id -> id >= 20),
        "fixture precondition: the global top 5 are all outside the filter; got " + globalTop5);
    probe("U5-2 full rows=%d globalTop5=%s lowBandTop5=%s", full.size(), globalTop5, expectedIds);

    // Filter before ranking: top-5 of the filtered rows.
    List<Map<String, Object>> prefiltered =
        query(direct, template, 5, null, false, "id < 20", true);
    assertEquals(expectedIds, ids(prefiltered), "prefilter=true must rank only the filtered rows");
    assertEquals(
        expectedScores, scores(prefiltered), "prefilter=true must return the filtered scores");
    probe(
        "U5-2 prefilter=true k=5 filter='id < 20' ids=%s scores=%s",
        ids(prefiltered), scores(prefiltered));

    // Postfilter: rank first, then drop non-matching rows -> empty because the global top 5 all
    // fail.
    List<Map<String, Object>> postfiltered =
        query(direct, template, 5, null, false, "id < 20", false);
    assertEquals(
        Collections.emptyList(),
        ids(postfiltered),
        "prefilter=false must rank first and then filter, leaving nothing here");
    probe("U5-2 prefilter=false k=5 filter='id < 20' ids=%s", ids(postfiltered));

    // The B' shape: a rowid range covering only the low band, bounded k, prefilter=true.
    Map<Long, Integer> rowIds = rowIdToId(full);
    long lowLo = Collections.min(rowIds.keySet());
    long lowHi =
        rowIds.entrySet().stream()
                .filter(e -> e.getValue() < 20)
                .mapToLong(Map.Entry::getKey)
                .max()
                .getAsLong()
            + 1;
    String rowIdFilter = "_rowid >= " + lowLo + " AND _rowid < " + lowHi;
    List<Map<String, Object>> bounded = query(direct, template, 5, null, false, rowIdFilter, true);
    assertEquals(
        expectedIds,
        ids(bounded),
        "a bounded rowid range + prefilter=true must return the exact top-5 of that range; filter="
            + rowIdFilter);
    probe(
        "U5-2 rowid range filter='%s' k=5 ids=%s scores=%s",
        rowIdFilter, ids(bounded), scores(bounded));
  }

  // ────────────────────────────────────────────────────────────────────────────
  // U5-3 — k=0 means "no limit" (and remains so with a filter)
  // ────────────────────────────────────────────────────────────────────────────

  @Test
  public void u5KZeroMeansUnlimited() throws Exception {
    RecordingDirectoryNamespace.reset();
    List<String> bodies = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      bodies.add("alpha beta");
    }
    for (int i = 0; i < 5; i++) {
      bodies.add("gamma delta");
    }
    String table = createFtsTable("u5_kzero", bodies, 2);
    QueryTableRequest template = captureSparkRequest(table);
    DirectoryNamespace direct = newDirectNamespace();

    List<Map<String, Object>> noFilter = query(direct, template, 0, null, false, null, true);
    assertEquals(25, noFilter.size(), "k=0 without a filter must return every match");
    probe("U5-3 k=0 no filter rows=%d", noFilter.size());

    List<Map<String, Object>> filtered = query(direct, template, 0, null, false, "id < 10", true);
    Set<Integer> expected =
        Arrays.stream(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
            .boxed()
            .collect(Collectors.toCollection(TreeSet::new));
    assertEquals(
        expected, sortedIds(filtered), "k=0 with a filter must return every filtered match");
    probe("U5-3 k=0 filter='id < 10' rows=%d ids=%s", filtered.size(), sortedIds(filtered));

    // And k=0 must not behave like "zero rows" even when a bounded rowid range is supplied.
    List<Map<String, Object>> ranged = query(direct, template, 0, null, true, "_rowid >= 0", true);
    assertEquals(25, ranged.size(), "k=0 with a rowid filter must still return every match");
    probe("U5-3 k=0 filter='_rowid >= 0' rows=%d", ranged.size());
  }

  // ────────────────────────────────────────────────────────────────────────────
  // U5-4 — rowid stability: append after the index, and partially-covered index
  // ────────────────────────────────────────────────────────────────────────────

  @Test
  public void u5RowIdsStayStableAcrossAppend() throws Exception {
    RecordingDirectoryNamespace.reset();
    String full = NS_CATALOG + ".default.u5_stability";
    spark.sql("CREATE TABLE " + full + " (id INT, body STRING) USING lance");
    insertIds(full, 0, 10, i -> "alpha beta gamma " + i);
    spark.sql(
        "ALTER TABLE "
            + full
            + " CREATE INDEX fts_body USING fts (body) WITH ("
            + FTS_INDEX_OPTIONS
            + ")");

    QueryTableRequest template = captureSparkRequest(full);
    DirectoryNamespace direct = newDirectNamespace();
    List<Map<String, Object>> before =
        query(direct, template, Integer.MAX_VALUE, null, true, null, true);
    Map<Integer, Long> rowIdsBefore =
        before.stream()
            .collect(
                Collectors.toMap(
                    r -> ((Number) r.get("id")).intValue(),
                    r -> asLong(r.get(LanceConstant.ROW_ID))));
    probe("U5-4 before append rows=%d fragments=%d", before.size(), fragmentCount(full));
    probe("U5-4 rowids before=%s", rowIdsBefore);

    // Append after the index exists: the new fragments are not covered by the FTS index, so this
    // is the "partial index" state a real table reaches after every append.
    insertIds(full, 10, 20, i -> "alpha beta gamma " + i);
    List<Map<String, Object>> after =
        query(direct, template, Integer.MAX_VALUE, null, true, null, true);
    Map<Integer, Long> rowIdsAfter =
        after.stream()
            .collect(
                Collectors.toMap(
                    r -> ((Number) r.get("id")).intValue(),
                    r -> asLong(r.get(LanceConstant.ROW_ID))));
    probe(
        "U5-4 after append rows=%d fragments=%d appendedRowsSeen=%d",
        after.size(),
        fragmentCount(full),
        rowIdsAfter.keySet().stream().filter(id -> id >= 10).count());

    for (int id = 0; id < 10; id++) {
      assertEquals(
          rowIdsBefore.get(id),
          rowIdsAfter.get(id),
          "appending new fragments must not renumber the rowids of existing rows (id=" + id + ")");
    }
    assertEquals(
        20,
        after.size(),
        "rows in unindexed (appended) fragments must still be found by the flat FTS path");
    probe("U5-4 rowids after=%s", rowIdsAfter);
    probe("U5-4 rowids stable for ids 0..9 across append; all 20 rows matched");

    LanceDataset table = (LanceDataset) loadTable(full);
    try (org.lance.Dataset dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      probe(
          "U5-4 native fragments=%d indexes=%s",
          dataset.getFragments().size(), dataset.listIndexes());
    }

    // Spark-side rowid/rowaddr view: non-stable rowids must equal row addresses, and equality
    // filters stored from an earlier snapshot must keep matching the same rows.
    List<Row> sparkRows =
        spark.sql("SELECT id, _rowid, _rowaddr FROM " + full + " ORDER BY id").collectAsList();
    int equal = 0;
    for (Row row : sparkRows) {
      long rowId = asLong(row.get(1));
      long rowAddr = asLong(row.get(2));
      if (rowId == rowAddr) {
        equal++;
      }
      if (rowIdsAfter.containsKey(row.getInt(0))) {
        assertEquals(
            rowIdsAfter.get(row.getInt(0)).longValue(),
            rowId,
            "Spark and queryTable must agree on _rowid (id=" + row.getInt(0) + ")");
      }
    }
    probe("U5-4 _rowid==_rowaddr for %d/%d rows", equal, sparkRows.size());
    assertEquals(
        0, sparkRows.size() - equal, "A non-stable-rowid dataset must expose _rowid==_rowaddr");
  }

  /**
   * The follow-up U5-4 probe: how do filters behave on fragments the FTS index does not cover?
   * Every line is recorded rather than asserted while the behaviour is still unknown; the known
   * good cases are asserted.
   */
  @Test
  public void u5FiltersOnUnindexedFragments() throws Exception {
    RecordingDirectoryNamespace.reset();
    String full = NS_CATALOG + ".default.u5_unindexed";
    spark.sql("CREATE TABLE " + full + " (id INT, body STRING) USING lance");
    insertIds(full, 0, 10, i -> "alpha beta gamma " + i);
    spark.sql(
        "ALTER TABLE "
            + full
            + " CREATE INDEX fts_body USING fts (body) WITH ("
            + FTS_INDEX_OPTIONS
            + ")");
    QueryTableRequest template = captureSparkRequest(full);
    DirectoryNamespace direct = newDirectNamespace();
    insertIds(full, 10, 20, i -> "alpha beta gamma " + i);

    List<Map<String, Object>> all =
        query(direct, template, Integer.MAX_VALUE, null, true, null, true);
    Map<Integer, Long> rowIds =
        all.stream()
            .collect(
                Collectors.toMap(
                    r -> ((Number) r.get("id")).intValue(),
                    r -> asLong(r.get(LanceConstant.ROW_ID))));
    assertEquals(20, all.size(), "the fixture must match all 20 rows");
    long indexedLo =
        rowIds.entrySet().stream()
            .filter(e -> e.getKey() < 10)
            .mapToLong(Map.Entry::getValue)
            .min()
            .getAsLong();
    long indexedHi =
        rowIds.entrySet().stream()
                .filter(e -> e.getKey() < 10)
                .mapToLong(Map.Entry::getValue)
                .max()
                .getAsLong()
            + 1;
    long appendedLo =
        rowIds.entrySet().stream()
            .filter(e -> e.getKey() >= 10)
            .mapToLong(Map.Entry::getValue)
            .min()
            .getAsLong();
    long appendedHi =
        rowIds.entrySet().stream()
                .filter(e -> e.getKey() >= 10)
                .mapToLong(Map.Entry::getValue)
                .max()
                .getAsLong()
            + 1;
    probe("U5-4b rowids=%s", rowIds);
    probe(
        "U5-4b indexedRange=[%d,%d) appendedRange=[%d,%d)",
        indexedLo, indexedHi, appendedLo, appendedHi);

    recordQuery(
        "U5-4b filter='id >= 10' prefilter=true",
        query(direct, template, Integer.MAX_VALUE, null, false, "id >= 10", true));
    recordQuery(
        "U5-4b filter='id < 10' prefilter=true",
        query(direct, template, Integer.MAX_VALUE, null, false, "id < 10", true));
    recordQuery(
        "U5-4b appended rowid range prefilter=true",
        query(
            direct,
            template,
            Integer.MAX_VALUE,
            null,
            true,
            rowRange(appendedLo, appendedHi),
            true));
    recordQuery(
        "U5-4b indexed rowid range prefilter=true",
        query(
            direct, template, Integer.MAX_VALUE, null, true, rowRange(indexedLo, indexedHi), true));
    recordQuery(
        "U5-4b appended rowid range prefilter=false",
        query(
            direct,
            template,
            Integer.MAX_VALUE,
            null,
            true,
            rowRange(appendedLo, appendedHi),
            false));
    recordQuery(
        "U5-4b indexed rowid range prefilter=false",
        query(
            direct,
            template,
            Integer.MAX_VALUE,
            null,
            true,
            rowRange(indexedLo, indexedHi),
            false));

    List<Long> appendedIds = new ArrayList<>();
    for (Map.Entry<Integer, Long> entry : rowIds.entrySet()) {
      if (entry.getKey() >= 10) {
        appendedIds.add(entry.getValue());
      }
    }
    appendedIds = appendedIds.subList(0, Math.min(3, appendedIds.size()));
    probe("U5-4b appended _rowid sample=%s", appendedIds);
    String inFilter =
        "_rowid IN ("
            + appendedIds.stream().map(String::valueOf).collect(Collectors.joining(", "))
            + ")";
    recordQuery(
        "U5-4b appended _rowid IN prefilter=true",
        query(direct, template, Integer.MAX_VALUE, null, true, inFilter, true));

    // Plain (non-FTS) scans in Spark, same table and a table without any index.
    List<Row> plainSpark =
        spark
            .sql("SELECT id FROM " + full + " WHERE _rowid >= " + appendedLo + " ORDER BY id")
            .collectAsList();
    probe(
        "U5-4b spark plain scan '_rowid >= %d' ids=%s",
        appendedLo, plainSpark.stream().map(r -> r.getInt(0)).collect(Collectors.toList()));
    List<Row> ftsSparkRange =
        spark
            .sql(
                "SELECT id FROM "
                    + full
                    + " WHERE lance_match(body, 'alpha') AND _rowid >= "
                    + appendedLo
                    + " ORDER BY id")
            .collectAsList();
    probe(
        "U5-4b spark fts + '_rowid >= %d' ids=%s",
        appendedLo, ftsSparkRange.stream().map(r -> r.getInt(0)).collect(Collectors.toList()));
    List<Row> ftsSparkColumnFilter =
        spark
            .sql(
                "SELECT id FROM "
                    + full
                    + " WHERE lance_match(body, 'alpha') AND id >= 10 ORDER BY id")
            .collectAsList();
    probe(
        "U5-4b spark fts + 'id >= 10' ids=%s",
        ftsSparkColumnFilter.stream().map(r -> r.getInt(0)).collect(Collectors.toList()));

    String noIndex = NS_CATALOG + ".default.u5_unindexed_no_fts";
    spark.sql("CREATE TABLE " + noIndex + " (id INT, body STRING) USING lance");
    insertIds(noIndex, 0, 10, i -> "alpha beta gamma " + i);
    insertIds(noIndex, 10, 20, i -> "alpha beta gamma " + i);
    List<Row> noIndexRange =
        spark
            .sql("SELECT id FROM " + noIndex + " WHERE _rowid >= " + appendedLo + " ORDER BY id")
            .collectAsList();
    probe(
        "U5-4b no-index plain scan '_rowid >= %d' ids=%s",
        appendedLo, noIndexRange.stream().map(r -> r.getInt(0)).collect(Collectors.toList()));
  }

  @Test
  public void u5OptimizeChangesRowIds() throws Exception {
    RecordingDirectoryNamespace.reset();
    String full = NS_CATALOG + ".default.u5_optimize";
    spark.sql("CREATE TABLE " + full + " (id INT, body STRING) USING lance");
    insertIds(full, 0, 10, i -> "alpha beta gamma " + i);
    insertIds(full, 10, 20, i -> "alpha beta gamma " + i);
    spark.sql(
        "ALTER TABLE "
            + full
            + " CREATE INDEX fts_body USING fts (body) WITH ("
            + FTS_INDEX_OPTIONS
            + ")");
    QueryTableRequest template = captureSparkRequest(full);
    DirectoryNamespace direct = newDirectNamespace();
    List<Map<String, Object>> before =
        query(direct, template, Integer.MAX_VALUE, null, true, null, true);
    Map<Integer, Long> beforeIds = idToRowId(before);
    probe("U5-4d fragments before optimize=%d rowids=%s", fragmentCount(full), beforeIds);

    spark.sql("OPTIMIZE " + full);
    List<Map<String, Object>> after =
        query(direct, template, Integer.MAX_VALUE, null, true, null, true);
    Map<Integer, Long> afterIds = idToRowId(after);
    probe("U5-4d fragments after optimize=%d rowids=%s", fragmentCount(full), afterIds);
    long changed =
        beforeIds.entrySet().stream()
            .filter(
                e ->
                    afterIds.containsKey(e.getKey())
                        && !e.getValue().equals(afterIds.get(e.getKey())))
            .count();
    probe("U5-4d rowids changed for %d/%d rows by OPTIMIZE", changed, beforeIds.size());

    // The stale range: if rowids moved, a saved rowid range no longer selects the same rows.
    long staleLo = beforeIds.values().stream().mapToLong(Long::longValue).min().getAsLong();
    long staleHi = beforeIds.values().stream().mapToLong(Long::longValue).max().getAsLong() + 1;
    List<Map<String, Object>> stale =
        query(direct, template, Integer.MAX_VALUE, null, true, rowRange(staleLo, staleHi), true);
    probe("U5-4d pre-optimize range rows=%d ids=%s", stale.size(), sortedIds(stale));
  }

  @Test
  public void u5StableRowIdsRangeFilter() throws Exception {
    RecordingDirectoryNamespace.reset();
    String full = NS_CATALOG + ".default.u5_stable_rowids";
    spark.sql(
        "CREATE TABLE "
            + full
            + " (id INT, body STRING) USING lance TBLPROPERTIES ('enable_stable_row_ids' = 'true')");
    insertIds(full, 0, 10, i -> "alpha beta gamma " + i);
    spark.sql(
        "ALTER TABLE "
            + full
            + " CREATE INDEX fts_body USING fts (body) WITH ("
            + FTS_INDEX_OPTIONS
            + ")");
    QueryTableRequest template = captureSparkRequest(full);
    DirectoryNamespace direct = newDirectNamespace();
    List<Map<String, Object>> rows =
        query(direct, template, Integer.MAX_VALUE, null, true, null, true);
    Map<Integer, Long> ids = idToRowId(rows);
    probe("U5-4e stable rowids=%s", ids);
    probe(
        "U5-4e stable rowid classes=%s",
        rows.get(0).get(LanceConstant.ROW_ID).getClass().getName());

    // Ranges must be expressed in unsigned decimal: rowid values are u64, and roughly half of
    // them exceed Long.MAX_VALUE.
    long lo = ids.values().stream().mapToLong(Long::longValue).min().getAsLong();
    long hi = ids.values().stream().mapToLong(Long::longValue).max().getAsLong();
    String loLiteral = Long.toUnsignedString(lo);
    String hiLiteral = Long.toUnsignedString(hi);
    probe("U5-4e signed lo=%d hi=%d unsigned lo=%s hi=%s", lo, hi, loLiteral, hiLiteral);
    try {
      List<Map<String, Object>> ranged =
          query(
              direct,
              template,
              Integer.MAX_VALUE,
              null,
              true,
              "_rowid >= " + loLiteral + " AND _rowid <= " + hiLiteral,
              true);
      probe("U5-4e stable rowid range rows=%d ids=%s", ranged.size(), sortedIds(ranged));
    } catch (RuntimeException e) {
      probe("U5-4e stable rowid range failed: %s: %s", e.getClass().getName(), e.getMessage());
    }
    try {
      List<Map<String, Object>> inRows =
          query(
              direct,
              template,
              Integer.MAX_VALUE,
              null,
              true,
              "_rowid IN ("
                  + ids.values().stream()
                      .limit(3)
                      .map(v -> Long.toUnsignedString(v))
                      .collect(Collectors.joining(", "))
                  + ")",
              true);
      probe("U5-4e stable rowid IN rows=%d ids=%s", inRows.size(), sortedIds(inRows));
    } catch (RuntimeException e) {
      probe("U5-4e stable rowid IN failed: %s: %s", e.getClass().getName(), e.getMessage());
    }
  }

  @Test
  public void u5PartialFragmentIndex() throws Exception {
    RecordingDirectoryNamespace.reset();
    String full = NS_CATALOG + ".default.u5_partial_index";
    spark.sql("CREATE TABLE " + full + " (id INT, body STRING) USING lance");
    insertIds(full, 0, 12, i -> "alpha beta gamma " + i);
    QueryTableRequest template = captureSparkRequest(full);
    DirectoryNamespace direct = newDirectNamespace();
    List<Map<String, Object>> all =
        query(direct, template, Integer.MAX_VALUE, null, true, null, true);
    assertEquals(12, all.size(), "fixture must match all 12 rows");
    Map<Integer, Long> rowIds =
        all.stream()
            .collect(
                Collectors.toMap(
                    r -> ((Number) r.get("id")).intValue(),
                    r -> asLong(r.get(LanceConstant.ROW_ID))));
    probe("U5-4c rowids=%s", rowIds);

    LanceDataset table = (LanceDataset) loadTable(full);
    List<Integer> fragmentIds;
    try (org.lance.Dataset dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      fragmentIds =
          dataset.getFragments().stream().map(f -> f.getId()).collect(Collectors.toList());
      assertTrue(fragmentIds.size() >= 2, "need at least two fragments; got " + fragmentIds);
      List<Integer> subset = new ArrayList<>(fragmentIds.subList(0, fragmentIds.size() - 1));
      try {
        dataset.createIndex(
            IndexOptions.builder(
                    Collections.singletonList("body"),
                    IndexType.INVERTED,
                    IndexParams.builder()
                        .setScalarIndexParams(ScalarIndexParams.create("inverted", ftsParamsJson()))
                        .build())
                .withIndexName("fts_body")
                .withFragmentIds(subset)
                .build());
        probe("U5-4c partial index fragments=%s of %d", subset, fragmentIds.size());
      } catch (RuntimeException e) {
        // Record instead of failing: the point of U5-4 is whether rowids stay valid, not whether
        // this particular native build path exists.
        probe("U5-4c partial index build failed: %s: %s", e.getClass().getName(), e.getMessage());
        return;
      }
    }

    List<Map<String, Object>> afterPartial =
        query(direct, template, Integer.MAX_VALUE, null, true, null, true);
    assertEquals(
        12,
        afterPartial.size(),
        "a partially-covered FTS index must still return matches from the uncovered fragments");
    List<Row> sparkRows =
        spark
            .sql("SELECT id, _score FROM " + full + " WHERE lance_match(body, 'alpha') ORDER BY id")
            .collectAsList();
    assertEquals(12, sparkRows.size(), "Spark path must also keep the uncovered rows");
    probe("U5-4c partial index query rows=%d ids=%s", afterPartial.size(), sortedIds(afterPartial));
  }

  // ────────────────────────────────────────────────────────────────────────────
  // helpers
  // ────────────────────────────────────────────────────────────────────────────

  private static Map<Integer, Long> idToRowId(List<Map<String, Object>> rows) {
    Map<Integer, Long> map = new LinkedHashMap<>();
    for (Map<String, Object> row : rows) {
      map.put(((Number) row.get("id")).intValue(), asLong(row.get(LanceConstant.ROW_ID)));
    }
    return map;
  }

  private static String rowRange(long lo, long hi) {
    return "_rowid >= " + lo + " AND _rowid < " + hi;
  }

  private void recordQuery(String label, List<Map<String, Object>> rows) {
    probe("%s rows=%d ids=%s", label, rows.size(), ids(rows));
  }

  private static String ftsParamsJson() {
    return "{\"base_tokenizer\":\"simple\",\"language\":\"English\",\"max_token_length\":40,"
        + "\"lower_case\":true,\"stem\":false,\"remove_stop_words\":false,"
        + "\"ascii_folding\":false,\"with_position\":true}";
  }

  private interface BodyFn {
    String apply(int id);
  }

  private void insertIds(String full, int from, int to, BodyFn fn) {
    StringBuilder values = new StringBuilder();
    for (int i = from; i < to; i++) {
      if (values.length() > 0) {
        values.append(", ");
      }
      values
          .append('(')
          .append(i)
          .append(", '")
          .append(fn.apply(i).replace("'", "''"))
          .append("')");
    }
    spark.sql("INSERT INTO " + full + " VALUES " + values);
  }

  private String createFtsTable(String name, List<String> bodies, int inserts) {
    String full = NS_CATALOG + ".default." + name;
    spark.sql("CREATE TABLE " + full + " (id INT, body STRING) USING lance");
    int perInsert = Math.max(1, (bodies.size() + inserts - 1) / inserts);
    for (int start = 0; start < bodies.size(); start += perInsert) {
      int end = Math.min(bodies.size(), start + perInsert);
      StringBuilder values = new StringBuilder();
      for (int i = start; i < end; i++) {
        if (values.length() > 0) {
          values.append(", ");
        }
        values
            .append('(')
            .append(i)
            .append(", '")
            .append(bodies.get(i).replace("'", "''"))
            .append("')");
      }
      spark.sql("INSERT INTO " + full + " VALUES " + values);
    }
    spark.sql(
        "ALTER TABLE "
            + full
            + " CREATE INDEX fts_body USING fts (body) WITH ("
            + FTS_INDEX_OPTIONS
            + ")");
    return full;
  }

  private QueryTableRequest captureSparkRequest(String fullTable) {
    RecordingDirectoryNamespace.reset();
    spark
        .sql("SELECT id, _score FROM " + fullTable + " WHERE lance_match(body, 'alpha')")
        .collectAsList();
    List<QueryTableRequest> requests = RecordingDirectoryNamespace.requests();
    assertFalse(requests.isEmpty(), "the Spark FTS scan must call queryTable");
    return requests.get(requests.size() - 1);
  }

  private static DirectoryNamespace newDirectNamespace() {
    Map<String, String> props = RecordingDirectoryNamespace.lastInitializeProperties();
    assertNotNull(props, "the catalog must have initialized the namespace at least once");
    RecordingDirectoryNamespace namespace = new RecordingDirectoryNamespace();
    namespace.initialize(props, LanceRuntime.allocator());
    return namespace;
  }

  private int fragmentCount(String fullTable) {
    LanceDataset table = (LanceDataset) loadTable(fullTable);
    try (org.lance.Dataset dataset = Utils.openDatasetBuilder(table.readOptions()).build()) {
      return dataset.getFragments().size();
    }
  }

  private Table loadTable(String fullTable) {
    String[] parts = fullTable.split("\\.");
    CatalogPlugin plugin = spark.sessionState().catalogManager().catalog(parts[0]);
    String[] namespace = Arrays.copyOfRange(parts, 1, parts.length - 1);
    try {
      return ((TableCatalog) plugin).loadTable(Identifier.of(namespace, parts[parts.length - 1]));
    } catch (Exception e) {
      throw new IllegalStateException("cannot load " + fullTable, e);
    }
  }

  private void printArrowSchema(DirectoryNamespace namespace, QueryTableRequest template)
      throws IOException {
    QueryTableRequest request = buildRequest(template, 1, null, true, null, true);
    QueryTableResponse response = namespace.queryTable(request);
    try (ArrowFileReader reader =
        new ArrowFileReader(
            new ByteArrayReadableSeekableByteChannel(response.getData()),
            LanceRuntime.allocator())) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      List<String> fields = new ArrayList<>();
      for (FieldVector vector : root.getFieldVectors()) {
        fields.add(vector.getField().getName() + ":" + vector.getField().getType());
      }
      probe("U5-1 response schema=%s", fields);
    }
  }

  private List<Map<String, Object>> query(
      DirectoryNamespace namespace,
      QueryTableRequest template,
      Integer k,
      Integer offset,
      boolean withRowId,
      String filter,
      boolean prefilter)
      throws IOException {
    QueryTableResponse response =
        namespace.queryTable(buildRequest(template, k, offset, withRowId, filter, prefilter));
    return decodeArrow(response.getData());
  }

  private static QueryTableRequest buildRequest(
      QueryTableRequest template,
      Integer k,
      Integer offset,
      boolean withRowId,
      String filter,
      boolean prefilter) {
    QueryTableRequest request = new QueryTableRequest();
    request.setId(new ArrayList<>(template.getId()));
    request.setColumns(
        new QueryTableRequestColumns()
            .columnNames(
                template.getColumns() == null
                    ? Arrays.asList("id", LanceConstant.SCORE)
                    : template.getColumns().getColumnNames()));
    request.setFullTextQuery(template.getFullTextQuery());
    // queryTable requires a non-null vector object even for a full-text request.
    request.setVector(new QueryTableRequestVector());
    request.setK(k);
    if (offset != null) {
      request.setOffset(offset);
    }
    if (filter != null) {
      request.setFilter(filter);
    }
    request.setPrefilter(prefilter);
    request.setWithRowId(withRowId);
    return request;
  }

  private static List<Map<String, Object>> decodeArrow(byte[] data) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<>();
    BufferAllocator allocator = LanceRuntime.allocator();
    try (ArrowFileReader reader =
        new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(data), allocator)) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        for (int row = 0; row < root.getRowCount(); row++) {
          Map<String, Object> values = new LinkedHashMap<>();
          for (FieldVector vector : root.getFieldVectors()) {
            values.put(vector.getField().getName(), vector.getObject(row));
          }
          rows.add(values);
        }
      }
    }
    return rows;
  }

  private static List<Integer> ids(List<Map<String, Object>> rows) {
    List<Integer> ids = new ArrayList<>();
    for (Map<String, Object> row : rows) {
      ids.add(((Number) row.get("id")).intValue());
    }
    return ids;
  }

  private static List<Float> scores(List<Map<String, Object>> rows) {
    List<Float> scores = new ArrayList<>();
    for (Map<String, Object> row : rows) {
      scores.add(((Number) row.get(LanceConstant.SCORE)).floatValue());
    }
    return scores;
  }

  private static Set<Integer> sortedIds(List<Map<String, Object>> rows) {
    return new TreeSet<>(ids(rows));
  }

  private static Map<Long, Integer> rowIdToId(List<Map<String, Object>> rows) {
    Map<Long, Integer> map = new LinkedHashMap<>();
    for (Map<String, Object> row : rows) {
      map.put(asLong(row.get(LanceConstant.ROW_ID)), ((Number) row.get("id")).intValue());
    }
    return map;
  }

  private static long asLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).longValue();
    }
    if (value instanceof java.math.BigInteger) {
      return ((java.math.BigInteger) value).longValue();
    }
    throw new IllegalArgumentException(
        "unexpected _rowid value "
            + value
            + " ("
            + (value == null ? "null" : value.getClass())
            + ")");
  }

  private static String repeat(String token, int times) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < times; i++) {
      if (i > 0) {
        builder.append(' ');
      }
      builder.append(token);
    }
    return builder.toString();
  }

  private void probe(String format, Object... args) {
    System.out.println("PROBE| " + String.format(Locale.ROOT, format, args));
  }

  /**
   * Records every queryTable request and the namespace properties so the probe can (a) reuse the
   * exact query Spark built and (b) open a second namespace handle for direct calls.
   */
  public static class RecordingDirectoryNamespace extends DirectoryNamespace {
    private static final List<QueryTableRequest> REQUESTS = new CopyOnWriteArrayList<>();
    private static volatile Map<String, String> initializeProperties;

    static void reset() {
      REQUESTS.clear();
    }

    static List<QueryTableRequest> requests() {
      return REQUESTS;
    }

    static Map<String, String> lastInitializeProperties() {
      return initializeProperties;
    }

    @Override
    public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
      initializeProperties = new HashMap<>(configProperties);
      super.initialize(configProperties, allocator);
    }

    @Override
    public QueryTableResponse queryTable(QueryTableRequest request) {
      REQUESTS.add(request);
      return super.queryTable(request);
    }
  }
}
