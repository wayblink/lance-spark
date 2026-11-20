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
package org.lance.spark.update;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.index.Index;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRuntime;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers {@code CREATE INDEX ... USING ivf_rq}: the DDL surface, the planning-time validation, and
 * the distributed segment build that gives every segment its own statistics.
 */
public abstract class BaseVectorIndexDdlTest {
  private static final String CATALOG_NAME = "lance_vec_ddl";
  private static final String INDEX_NAME = "vec_rq_idx";

  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-vector-index-ddl-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG_NAME + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG_NAME + ".default");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  // ---------------------------------------------------------------- build

  @Test
  void createsOneIndependentSegmentPerBatch() throws Exception {
    String table = createVectorTable("vec_build", 8, 4);

    List<Row> result =
        spark
            .sql(
                "ALTER TABLE "
                    + table
                    + " CREATE INDEX "
                    + INDEX_NAME
                    + " USING ivf_rq (vector) WITH (num_bits = 8, num_segments = 2)")
            .collectAsList();

    assertEquals(1, result.size(), "CREATE INDEX returns one row");
    assertEquals(4L, result.get(0).getLong(0), "every fragment must be indexed");
    assertEquals(INDEX_NAME, result.get(0).getString(1));

    assertTrue(listIndexes(table).contains(INDEX_NAME), "index must be visible after commit");

    List<Index> segments = segmentsOf(table);
    assertEquals(2, segments.size(), "num_segments=2 must produce two independent segments");

    Set<Integer> covered = new HashSet<>();
    for (Index segment : segments) {
      List<Integer> coverage = segment.fragments().orElseThrow(IllegalStateException::new);
      assertTrue(!coverage.isEmpty(), "each segment must record its fragment coverage");
      for (Integer fragmentId : coverage) {
        assertTrue(covered.add(fragmentId), "fragment " + fragmentId + " covered twice");
      }
    }
    assertEquals(new HashSet<>(fragmentIds(table)), covered, "segments must cover every fragment");
  }

  @Test
  void defaultOptionsSplitByDefaultParallelism() throws Exception {
    String table = createVectorTable("vec_defaults", 8, 3);

    spark.sql("ALTER TABLE " + table + " CREATE INDEX " + INDEX_NAME + " USING ivf_rq (vector)");

    int parallelism = spark.sparkContext().defaultParallelism();
    assertEquals(
        Math.min(fragmentIds(table).size(), parallelism),
        segmentsOf(table).size(),
        "default num_segments must match the scalar build's min(fragments, parallelism) split");
  }

  @Test
  void numSegmentsIsClampedToTheFragmentCount() throws Exception {
    String table = createVectorTable("vec_clamp", 8, 4);

    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_segments = 99)");

    List<Index> segments = segmentsOf(table);
    assertEquals(
        fragmentIds(table).size(),
        segments.size(),
        "num_segments above the fragment count clamps to one segment per fragment");
  }

  @Test
  void numSegmentsOnePutsEveryFragmentInOneSegment() throws Exception {
    String table = createVectorTable("vec_one_segment", 8, 4);

    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_segments = 1)");

    List<Index> segments = segmentsOf(table);
    assertEquals(1, segments.size(), "num_segments=1 must build a single segment");
    assertEquals(
        new HashSet<>(fragmentIds(table)),
        new HashSet<>(segments.get(0).fragments().orElseThrow(IllegalStateException::new)),
        "the single segment must cover every fragment");
  }

  // ---------------------------------------------------------------- validation

  @Test
  void rejectsMoreThanOneColumn() throws Exception {
    String table = createVectorTable("vec_multi_column", 8, 2);

    Throwable error =
        assertThrows(
            Exception.class,
            () ->
                spark.sql("ALTER TABLE " + table + " CREATE INDEX bad USING ivf_rq (vector, id)"));
    assertTrue(
        rootMessage(error).contains("single column"),
        "error must state that one column is supported, got: " + rootMessage(error));
  }

  @Test
  void buildsIndexOnATableWhoseVectorsIncludeNulls() throws Exception {
    String table = createVectorTableWithNulls("vec_nulls", 8, 2);

    List<Row> result =
        spark
            .sql(
                "ALTER TABLE "
                    + table
                    + " CREATE INDEX "
                    + INDEX_NAME
                    + " USING ivf_rq (vector) WITH (num_bits = 8, num_segments = 1)")
            .collectAsList();

    assertEquals(1, result.size(), "CREATE INDEX returns one row");
    assertTrue(
        listIndexes(table).contains(INDEX_NAME),
        "a table that holds NULL vectors must still get its index committed");
    assertEquals(
        new HashSet<>(fragmentIds(table)),
        coveredFragments(table),
        "the index must cover every fragment, not only the ones without NULL vectors");
  }

  @Test
  void rejectsDimensionNotDivisibleByEight() throws Exception {
    String table = createVectorTable("vec_bad_dim", 4, 2);

    Throwable error =
        assertThrows(
            Exception.class,
            () -> spark.sql("ALTER TABLE " + table + " CREATE INDEX bad USING ivf_rq (vector)"));
    String message = rootMessage(error);
    assertTrue(
        message.contains("divisible by 8"),
        "error must name the RaBitQ constraint, got: " + message);
    assertTrue(message.contains("4"), "error must report the actual dimension, got: " + message);
  }

  @Test
  void rejectsNonFixedSizeListColumn() throws Exception {
    String table = createListTable("vec_bad_type");

    Throwable error =
        assertThrows(
            Exception.class,
            () -> spark.sql("ALTER TABLE " + table + " CREATE INDEX bad USING ivf_rq (vector)"));
    String message = rootMessage(error);
    assertTrue(
        message.contains("fixed-size list"),
        "error must ask for a fixed-size list, got: " + message);
  }

  @Test
  void rejectsNumBitsOutOfRange() throws Exception {
    String table = createVectorTable("vec_bad_bits", 8, 2);

    Throwable error =
        assertThrows(
            Exception.class,
            () ->
                spark.sql(
                    "ALTER TABLE "
                        + table
                        + " CREATE INDEX bad USING ivf_rq (vector) WITH (num_bits = 9)"));
    String message = rootMessage(error);
    assertTrue(message.contains("num_bits"), "error must name num_bits, got: " + message);
    assertTrue(message.contains("8"), "error must state the upper bound, got: " + message);
  }

  @Test
  void rejectsUnknownOption() throws Exception {
    String table = createVectorTable("vec_bad_option", 8, 2);

    Throwable error =
        assertThrows(
            Exception.class,
            () ->
                spark.sql(
                    "ALTER TABLE "
                        + table
                        + " CREATE INDEX bad USING ivf_rq (vector) WITH (nprobes = 16)"));
    String message = rootMessage(error);
    assertTrue(
        message.contains("nprobes"),
        "error must name the rejected option instead of ignoring it, got: " + message);
  }

  /**
   * A segment may train more than one IVF partition from its own fragments. The build must succeed
   * and record the count, because REFRESH INDEX has to reproduce the same partition layout.
   */
  @Test
  void buildsMultiplePartitionsPerSegment() throws Exception {
    String table = createVectorTable("vec_multi_partition", 8, 8);

    List<Row> result =
        spark
            .sql(
                "ALTER TABLE "
                    + table
                    + " CREATE INDEX "
                    + INDEX_NAME
                    + " USING ivf_rq (vector) WITH (num_partitions = 4, num_bits = 8, "
                    + "num_segments = 2)")
            .collectAsList();

    assertEquals(1, result.size(), "CREATE INDEX returns one row");
    assertEquals(8L, result.get(0).getLong(0), "every fragment must be indexed");
    assertEquals(2, segmentsOf(table).size(), "num_segments=2 must produce two segments");
    assertTrue(
        vectorParams(table).contains("\"num_partitions\":4"),
        "the build must record the partition count, got: " + vectorParams(table));
  }

  /**
   * A segment smaller than its recorded partition count is what REFRESH INDEX produces when a large
   * index receives a small new fragment: the build must reuse sample rows instead of failing.
   */
  @Test
  void buildsWithMorePartitionsThanRows() throws Exception {
    String table = createVectorTable("vec_more_partitions", 8, 8);

    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_partitions = 16, num_segments = 8)");

    assertTrue(listIndexes(table).contains(INDEX_NAME), "index must be visible after commit");
    assertEquals(8, segmentsOf(table).size(), "one segment per fragment");
    assertEquals(
        new HashSet<>(fragmentIds(table)),
        coveredFragments(table),
        "every fragment must be covered");
  }

  /** Cosine segments normalise the sample before clustering; the native build must accept it. */
  @Test
  void buildsMultiPartitionCosineIndex() throws Exception {
    String table = createVectorTable("vec_cosine_partitions", 8, 4);

    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_partitions = 4, metric = 'cosine', "
            + "num_segments = 2)");

    assertTrue(listIndexes(table).contains(INDEX_NAME), "index must be visible after commit");
    assertEquals(2, segmentsOf(table).size(), "num_segments=2 must produce two segments");
  }

  /** Two segments of 32 rows each: the default partition count is sqrt(32), rounded. */
  @Test
  void defaultPartitionsFollowTheSquareRootOfSegmentRows() throws Exception {
    String table = createVectorTable("vec_default_partitions", 8, 8);

    // One segment of 64 rows: sqrt(64) = 8 IVF partitions.
    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_segments = 1)");

    assertTrue(
        vectorParams(table).contains("\"num_partitions\":8"),
        "default partitions must follow sqrt(rows per segment), got: " + vectorParams(table));
  }

  @Test
  void rejectsDeferredCreation() throws Exception {
    String table = createVectorTable("vec_train_false", 8, 2);

    Throwable error =
        assertThrows(
            Exception.class,
            () ->
                spark.sql(
                    "ALTER TABLE "
                        + table
                        + " CREATE INDEX bad USING ivf_rq (vector) WITH (train = false)"));
    String message = rootMessage(error);
    assertTrue(
        message.contains("train=false"),
        "error must explain that a deferred vector index is unsupported, got: " + message);
  }

  // ---------------------------------------------------------------- maintenance

  @Test
  void refreshIndexesOnlyTheNewFragments() throws Exception {
    String table = createVectorTable("vec_refresh", 8, 4);
    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_bits = 8, num_segments = 1)");
    assertEquals(4, coveredFragments(table).size(), "the initial build covers four fragments");

    appendFragments(table, 8, 100, 2);
    assertEquals(6, fragmentIds(table).size(), "two fragments were appended");

    List<Row> refreshed =
        spark.sql("ALTER TABLE " + table + " REFRESH INDEX " + INDEX_NAME).collectAsList();

    assertEquals(1, refreshed.size(), "REFRESH INDEX returns one row");
    assertEquals(2L, refreshed.get(0).getLong(0), "only the two new fragments are indexed");
    assertEquals(
        new HashSet<>(fragmentIds(table)),
        coveredFragments(table),
        "after the refresh the index must cover every fragment");
  }

  @Test
  void refreshAcceptsParametersThatMatchTheRecordedBuild() throws Exception {
    String table = createVectorTable("vec_refresh_match", 8, 4);
    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_bits = 8, num_segments = 1)");
    appendFragments(table, 8, 100, 1);

    spark.sql("ALTER TABLE " + table + " REFRESH INDEX " + INDEX_NAME + " WITH (num_bits = 8)");

    assertEquals(
        new HashSet<>(fragmentIds(table)),
        coveredFragments(table),
        "a matching option must let the refresh proceed");
  }

  @Test
  void refreshRejectsParametersThatDifferFromTheBuild() throws Exception {
    String table = createVectorTable("vec_refresh_conflict", 8, 4);
    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_bits = 4, num_segments = 1)");
    appendFragments(table, 8, 100, 1);

    Throwable error =
        assertThrows(
            Exception.class,
            () ->
                spark.sql(
                    "ALTER TABLE "
                        + table
                        + " REFRESH INDEX "
                        + INDEX_NAME
                        + " WITH (num_bits = 8)"));
    String message = rootMessage(error);
    assertTrue(message.contains("num_bits"), "error must name the option, got: " + message);
    assertTrue(
        message.contains("conflicts with 4"),
        "error must quote the recorded value, got: " + message);
  }

  @Test
  void refreshRejectsAPartitionCountThatDiffersFromTheBuild() throws Exception {
    String table = createVectorTable("vec_refresh_partitions", 8, 8);
    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_partitions = 4, num_segments = 1)");
    appendFragments(table, 8, 100, 1);

    Throwable error =
        assertThrows(
            Exception.class,
            () ->
                spark.sql(
                    "ALTER TABLE "
                        + table
                        + " REFRESH INDEX "
                        + INDEX_NAME
                        + " WITH (num_partitions = 8)"));
    String message = rootMessage(error);
    assertTrue(message.contains("num_partitions"), "error must name the option, got: " + message);
    assertTrue(
        message.contains("conflicts with 4"),
        "error must quote the recorded value, got: " + message);
  }

  @Test
  void refreshRejectsAnIndexWithoutRecordedParameters() throws Exception {
    String table = createVectorTable("vec_refresh_unrecorded", 8, 4);
    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_segments = 1)");
    spark.sql(
        "ALTER TABLE " + table + " UNSET TBLPROPERTIES ('lance.index." + INDEX_NAME + ".vector')");
    appendFragments(table, 8, 100, 1);

    Throwable error =
        assertThrows(
            Exception.class,
            () -> spark.sql("ALTER TABLE " + table + " REFRESH INDEX " + INDEX_NAME));
    String message = rootMessage(error);
    assertTrue(
        message.contains("no recorded build parameters"),
        "error must explain the index cannot reproduce its build, got: " + message);
  }

  /**
   * Lance merges vector segments only when they share IVF centroids, which segment-local statistics
   * deliberately do not. The command must say so instead of letting the native build fail.
   */
  @Test
  void optimizeRejectsVectorSegmentsBecauseCentroidsDiffer() throws Exception {
    String table = createVectorTable("vec_optimize", 8, 4);
    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_segments = 4)");
    assertEquals(4, segmentsOf(table).size(), "num_segments=4 must produce four segments");

    Throwable error =
        assertThrows(
            Exception.class,
            () ->
                spark.sql(
                    "ALTER TABLE "
                        + table
                        + " OPTIMIZE INDEX "
                        + INDEX_NAME
                        + " WITH (num_indices_to_merge = 4)"));
    String message = rootMessage(error);
    assertTrue(
        message.contains("share IVF centroids"), "error must give the reason, got: " + message);

    assertEquals(4, segmentsOf(table).size(), "a rejected optimize must not change the index");
    assertEquals(
        new HashSet<>(fragmentIds(table)), coveredFragments(table), "coverage must be untouched");
  }

  // ---------------------------------------------------------------- helpers

  private String createVectorTable(String name, int dimension, int fragments) {
    String fullName = CATALOG_NAME + ".default." + name;
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '"
            + dimension
            + "')");
    for (int fragment = 0; fragment < fragments; fragment++) {
      int start = fragment * 8;
      spark.sql(
          "INSERT INTO "
              + fullName
              + " SELECT CAST(id AS INT), transform(sequence(1, "
              + dimension
              + "), x -> CAST(id AS FLOAT)) FROM range("
              + start
              + ", "
              + (start + 8)
              + ", 1, 1)");
    }
    return fullName;
  }

  /** A vector table whose rows include rows with no vector at all (NULL, not a zero vector). */
  private String createVectorTableWithNulls(String name, int dimension, int fragments) {
    String fullName = CATALOG_NAME + ".default." + name;
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, vector ARRAY<FLOAT>) USING lance "
            + "TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '"
            + dimension
            + "')");
    for (int fragment = 0; fragment < fragments; fragment++) {
      int start = fragment * 8;
      spark.sql(
          "INSERT INTO "
              + fullName
              + " SELECT CAST(id AS INT), CASE WHEN id % 3 = 0 THEN NULL ELSE"
              + " transform(sequence(1, "
              + dimension
              + "), x -> CAST(id AS FLOAT)) END FROM range("
              + start
              + ", "
              + (start + 8)
              + ", 1, 1)");
    }
    return fullName;
  }

  /** A plain array column: no fixed-size-list metadata, so it cannot carry a vector index. */
  private String createListTable(String name) {
    String fullName = CATALOG_NAME + ".default." + name;
    spark.sql(
        "CREATE TABLE "
            + fullName
            + " (id INT NOT NULL, vector ARRAY<FLOAT> NOT NULL) USING lance");
    spark.sql(
        "INSERT INTO "
            + fullName
            + " SELECT CAST(id AS INT), array(CAST(id AS FLOAT), 1.0) FROM range(0, 8, 1, 1)");
    return fullName;
  }

  private void appendFragments(String table, int dimension, int startId, int fragments) {
    for (int fragment = 0; fragment < fragments; fragment++) {
      int start = startId + fragment * 8;
      spark.sql(
          "INSERT INTO "
              + table
              + " SELECT CAST(id AS INT), transform(sequence(1, "
              + dimension
              + "), x -> CAST(id AS FLOAT)) FROM range("
              + start
              + ", "
              + (start + 8)
              + ", 1, 1)");
    }
  }

  private Set<Integer> coveredFragments(String table) throws Exception {
    Set<Integer> covered = new HashSet<>();
    for (Index index : segmentsOf(table)) {
      for (Integer fragmentId : index.fragments().orElseThrow(IllegalStateException::new)) {
        covered.add(fragmentId);
      }
    }
    return covered;
  }

  private List<String> listIndexes(String table) throws Exception {
    try (Dataset ds = openDataset(table)) {
      return ds.listIndexes();
    }
  }

  private List<Index> segmentsOf(String table) throws Exception {
    try (Dataset ds = openDataset(table)) {
      List<Index> segments = new ArrayList<>();
      for (Index index : ds.getIndexes()) {
        if (INDEX_NAME.equals(index.name())) {
          segments.add(index);
        }
      }
      return segments;
    }
  }

  private List<Integer> fragmentIds(String table) throws Exception {
    try (Dataset ds = openDataset(table)) {
      List<Integer> ids = new ArrayList<>();
      for (Fragment fragment : ds.getFragments()) {
        ids.add(fragment.getId());
      }
      Collections.sort(ids);
      return ids;
    }
  }

  private Dataset openDataset(String table) throws Exception {
    return Dataset.open().allocator(LanceRuntime.allocator()).uri(datasetUriOf(table)).build();
  }

  /** The JSON build parameters CREATE INDEX recorded for {@link #INDEX_NAME}. */
  private String vectorParams(String table) {
    return spark.sql("SHOW TBLPROPERTIES " + table).collectAsList().stream()
        .filter(row -> ("lance.index." + INDEX_NAME + ".vector").equals(row.getString(0)))
        .map(row -> row.getString(1))
        .findFirst()
        .orElseThrow(() -> new AssertionError("index build parameters were not recorded"));
  }

  private String datasetUriOf(String table) throws Exception {
    String name = table.substring(table.lastIndexOf('.') + 1);
    LanceDataset lanceTable =
        (LanceDataset)
            ((TableCatalog) spark.sessionState().catalogManager().catalog(CATALOG_NAME))
                .loadTable(Identifier.of(new String[] {"default"}, name));
    return lanceTable.readOptions().getDatasetUri();
  }

  private String rootMessage(Throwable throwable) {
    Throwable root = throwable;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return String.valueOf(root.getMessage());
  }
}
