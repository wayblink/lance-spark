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
import org.lance.Fragment;
import org.lance.index.DistanceType;
import org.lance.index.Index;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.vector.IvfBuildParams;
import org.lance.index.vector.RQBuildParams;
import org.lance.index.vector.VectorIndexParams;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRuntime;
import org.lance.spark.utils.VectorUtils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * V1 spike for the IVF + RaBitQ segment-local index (#3015/#3016): can a distributed build produce
 * one independent index segment per fragment batch, each carrying only its own statistics, and can
 * the existing distributed {@code VECTOR_SEARCH} answer correctly from it?
 *
 * <p>No DDL exists yet (blocked on #605), so segments are built directly through the Lance Java API
 * exactly the way the production connector will: driver trains nothing, each segment gets centroids
 * computed from its own fragments only, and the driver commits the segments into one named index.
 *
 * <p>The reference answer comes from the same SQL query with {@code bypass_vector_index => true},
 * which plans one exact flat task per fragment. Both paths run with distributed search enabled, so
 * the comparison isolates the index, not the execution mode.
 *
 * <p>Set {@code -Dlance.spike.matrix=true} to run the full {@code num_bits} x {@code refine_factor}
 * recall matrix. It is off by default so CI does not pay for it; the numbers it prints are pasted
 * into {@code docs/design/vector-index-ddl.md}.
 */
public abstract class BaseSegmentLocalIvfRqTest {
  private static final String CATALOG_NAME = "lance_seg_rq";
  private static final String VECTOR_COLUMN = "vector";
  private static final String INDEX_NAME = "vec_rq_idx";

  /** RaBitQ requires {@code dimension % 8 == 0}; 128 is a realistic embedding width. */
  private static final int DIM = 128;

  private static final int FRAGMENT_ROWS = 250;
  private static final int FRAGMENT_COUNT = 8;
  private static final int TOTAL_ROWS = FRAGMENT_ROWS * FRAGMENT_COUNT;

  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-segment-local-ivf-rq-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG_NAME + ".root", tempDir.toString())
            .config("spark.sql.lance.search.distributed.enabled", "true")
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG_NAME + ".default");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  // ---------------------------------------------------------------- tests

  /**
   * The mapping this design relies on: each segment covers a contiguous run of fragments, the runs
   * are a partition of the table (disjoint, and together every fragment).
   */
  @Test
  void segmentsPartitionTheFragments() throws Exception {
    String table = createTable("rq_partition");
    List<Integer> fragments = fragmentIds(table);
    assertEquals(FRAGMENT_COUNT, fragments.size(), "one fragment per insert batch");

    List<List<Integer>> batches = contiguousBatches(fragments, 2);
    buildSegments(table, batches, DIM, 4);

    List<Index> segments = segmentMetadata(table);
    assertEquals(2, segments.size(), "two batches must produce two independent segments");

    Set<Integer> covered = new LinkedHashSet<>();
    for (Index segment : segments) {
      List<Integer> coverage = segment.fragments().orElseThrow(IllegalStateException::new);
      assertFalse(coverage.isEmpty(), "a segment must record its fragment coverage");
      for (Integer fragmentId : coverage) {
        assertTrue(covered.add(fragmentId), "fragment " + fragmentId + " covered by two segments");
      }
    }
    assertEquals(
        new HashSet<>(fragments), covered, "the segments together must cover every fragment");
  }

  /**
   * Correctness claim of the design, in the form the data supports: 8 bits with refine 10 returns
   * exactly the same rows as the exact scan. Merging per-segment top-k by true distance is then
   * exact, so the global answer does not depend on segment count or on segment-local statistics.
   */
  @Test
  void eightBitsWithRefineTenMatchesTheExactScan() throws Exception {
    String table = createTable("rq_exact_8bit");
    List<Integer> fragments = fragmentIds(table);

    buildSegments(table, contiguousBatches(fragments, 2), DIM, 8);

    for (List<Float> query : queryVectors()) {
      List<Integer> exact = search(table, query, 10, null, true, 1);
      List<Integer> approx = search(table, query, 10, 10, false, 1);
      assertEquals(
          exact, approx, "8-bit with refine_factor=10 must reproduce the exact scan row for row");
    }
  }

  /**
   * A segment may train several IVF partitions from its own rows: the build must accept the
   * multi-centroid model and the index must still answer with near-exact recall. The assertion is a
   * recall floor rather than row-for-row equality because RaBitQ draws its rotation per build.
   */
  @Test
  void multiPartitionSegmentsStayNearTheExactScan() throws Exception {
    String table = createTable("rq_multi_partition");
    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_partitions = 8, num_bits = 8, num_segments = 2)");
    spark.sql("REFRESH TABLE " + table);
    assertEquals(2, segmentMetadata(table).size(), "num_segments=2 must produce two segments");

    double recall = meanRecall(table, 8, 10, 10, 8);
    assertTrue(
        recall >= 0.9,
        "8-bit multi-partition segments with refine_factor=10 must stay >= 0.9 recall, got: "
            + recall);
  }

  /**
   * What a user gets with no tuning knobs at all: the DDL defaults (`num_partitions` from sqrt(rows
   * per segment)) and the query defaults (`nprobes` unset). This is the configuration the
   * documentation points people at, so it needs its own recall floor.
   */
  @Test
  void defaultConfigurationStaysNearTheExactScan() throws Exception {
    String table = createTable("rq_default_partitions");
    spark.sql(
        "ALTER TABLE "
            + table
            + " CREATE INDEX "
            + INDEX_NAME
            + " USING ivf_rq (vector) WITH (num_bits = 8, num_segments = 2)");
    spark.sql("REFRESH TABLE " + table);

    double recall = meanRecall(table, 8, 10, 10, null);
    assertTrue(
        recall >= 0.9,
        "the default num_partitions and query defaults must stay >= 0.9 recall, got: " + recall);
  }

  /**
   * The spike's other finding: 1 bit is usable only with a wide refine, and even then it is near
   * exact rather than exact - RaBitQ draws its rotation signs per build, so two builds of the same
   * data differ slightly. This test pins both halves so a future change cannot quietly turn 1 bit
   * into a default without the refine that makes it viable.
   */
  @Test
  void oneBitNeedsWideRefineAndStaysNearExact() throws Exception {
    String table = createTable("rq_1bit");
    List<Integer> fragments = fragmentIds(table);
    buildSegments(table, contiguousBatches(fragments, 2), DIM, 1);

    double narrow = meanRecall(table, 1, 1, 10, 1);
    double wide = meanRecall(table, 1, 30, 10, 1);
    assertTrue(narrow <= 0.9, "1 bit with refine_factor=1 must not pass as usable: " + narrow);
    assertTrue(wide >= 0.95, "1 bit needs refine_factor=30 to stay near exact: " + wide);
  }

  /**
   * Recall matrix for choosing defaults. Asserts the one configuration we intend to ship as the
   * documented default (8 bits, refine 10) stays at or above 0.9, and prints the rest for the
   * design doc. Run with {@code -Dlance.spike.matrix=true} for the full sweep.
   */
  @Test
  void recallMatrixForDefaults() throws Exception {
    String table = createTable("rq_matrix");
    List<Integer> fragments = fragmentIds(table);
    List<List<Integer>> batches = contiguousBatches(fragments, 2);

    boolean fullSweep = Boolean.getBoolean("lance.spike.matrix");
    int[] bitWidths = fullSweep ? new int[] {1, 4, 8} : new int[] {8};
    int[] refineFactors = fullSweep ? new int[] {1, 10, 30} : new int[] {10};

    System.out.println("| segments | num_bits | refine_factor | recall@10 |");
    System.out.println("|---|---|---|---|");
    for (int bits : bitWidths) {
      buildSegments(table, batches, DIM, bits);
      for (int refine : refineFactors) {
        double recall = meanRecall(table, bits, refine, 10, 1);
        System.out.println(
            "| " + batches.size() + " | " + bits + " | " + refine + " | " + recall + " |");
        if (bits == 8 && refine == 10) {
          assertTrue(
              recall >= 0.9, "8-bit with refine_factor=10 must stay >= 0.9 recall, got " + recall);
        }
      }
      dropIndex(table);
    }
  }

  // ---------------------------------------------------------------- helpers: data

  private String createTable(String name) throws Exception {
    String fullName = CATALOG_NAME + ".default." + name;
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  VECTOR_COLUMN,
                  DataTypes.createArrayType(DataTypes.FloatType, false),
                  false,
                  new MetadataBuilder()
                      .putLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY, DIM)
                      .build())
            });

    // One insert per fragment: a coalesced write produces exactly one fragment, which is what lets
    // the test know a fragment's rows without reading them back.
    for (int fragment = 0; fragment < FRAGMENT_COUNT; fragment++) {
      List<Row> rows = new ArrayList<>();
      for (int id = fragment * FRAGMENT_ROWS; id < (fragment + 1) * FRAGMENT_ROWS; id++) {
        rows.add(RowFactory.create(id, vectorOf(id)));
      }
      org.apache.spark.sql.Dataset<Row> batch = spark.createDataFrame(rows, schema).coalesce(1);
      if (fragment == 0) {
        batch.writeTo(fullName).using("lance").create();
      } else {
        batch.writeTo(fullName).append();
      }
    }
    return fullName;
  }

  /** Deterministic pseudo-random vector, so a failure is reproducible. */
  private static List<Float> vectorOf(int id) {
    long state = id * 6364136223846793005L + 1442695040888963407L;
    List<Float> vector = new ArrayList<>(DIM);
    for (int i = 0; i < DIM; i++) {
      state = state * 6364136223846793005L + 1442695040888963407L;
      float value = (float) (((state >>> 11) % 100_000L) / 100_000.0 * 2.0 - 1.0);
      vector.add(value);
    }
    return vector;
  }

  /** Five query vectors, each an actual row of the table. */
  private List<List<Float>> queryVectors() {
    List<List<Float>> queries = new ArrayList<>();
    for (int id : new int[] {17, 249, 512, 1_001, 1_877}) {
      queries.add(vectorOf(id));
    }
    return queries;
  }

  // ---------------------------------------------------------------- helpers: index

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

  private List<Index> segmentMetadata(String table) throws Exception {
    try (Dataset ds = openDataset(table)) {
      return ds.getIndexes().stream()
          .filter(index -> INDEX_NAME.equals(index.name()))
          .collect(Collectors.toList());
    }
  }

  private static List<List<Integer>> contiguousBatches(List<Integer> fragments, int count) {
    List<Integer> sorted = new ArrayList<>(fragments);
    Collections.sort(sorted);
    int actual = Math.max(1, Math.min(count, sorted.size()));
    List<List<Integer>> batches = new ArrayList<>();
    int base = sorted.size() / actual;
    int remainder = sorted.size() % actual;
    int cursor = 0;
    for (int i = 0; i < actual; i++) {
      int size = base + (i < remainder ? 1 : 0);
      batches.add(new ArrayList<>(sorted.subList(cursor, cursor + size)));
      cursor += size;
    }
    return batches;
  }

  /**
   * Builds one IVF_RQ segment per batch. Centroids come from that batch's own rows only - the
   * production task will read them back from the fragment; here the test knows them because it
   * wrote them, which keeps the spike free of Arrow plumbing.
   */
  private void buildSegments(String table, List<List<Integer>> batches, int dimension, int numBits)
      throws Exception {
    dropIndex(table);
    try (Dataset ds = openDataset(table)) {
      List<Index> segments = new ArrayList<>();
      for (List<Integer> batch : batches) {
        float[][] centroids = new float[1][dimension];
        for (Integer fragmentId : batch) {
          for (int id = fragmentId * FRAGMENT_ROWS; id < (fragmentId + 1) * FRAGMENT_ROWS; id++) {
            List<Float> vector = vectorOf(id);
            for (int i = 0; i < dimension; i++) {
              centroids[0][i] += vector.get(i);
            }
          }
        }
        int rows = batch.size() * FRAGMENT_ROWS;
        for (int i = 0; i < dimension; i++) {
          centroids[0][i] /= rows;
        }

        IvfBuildParams ivfParams =
            new IvfBuildParams.Builder()
                .setNumPartitions(1)
                .setMaxIters(1)
                .setCentroids(flatten(centroids))
                .build();
        RQBuildParams rqParams = new RQBuildParams.Builder().setNumBits((byte) numBits).build();
        IndexParams params =
            IndexParams.builder()
                .setVectorIndexParams(
                    new VectorIndexParams.Builder(ivfParams)
                        .setDistanceType(DistanceType.L2)
                        .setRqParams(rqParams)
                        .build())
                .build();

        segments.add(
            ds.createIndex(
                IndexOptions.builder(
                        Collections.singletonList(VECTOR_COLUMN), IndexType.IVF_RQ, params)
                    .withIndexName(INDEX_NAME)
                    .replace(true)
                    .withFragmentIds(batch)
                    .build()));
      }
      ds.commitExistingIndexSegments(INDEX_NAME, VECTOR_COLUMN, segments);
    }
    spark.sql("REFRESH TABLE " + table);
  }

  private static float[] flatten(float[][] centroids) {
    float[] flat = new float[centroids.length * centroids[0].length];
    for (int row = 0; row < centroids.length; row++) {
      System.arraycopy(centroids[row], 0, flat, row * centroids[0].length, centroids[0].length);
    }
    return flat;
  }

  private void dropIndex(String table) throws Exception {
    try (Dataset ds = openDataset(table)) {
      if (ds.listIndexes().contains(INDEX_NAME)) {
        ds.dropIndex(INDEX_NAME);
      }
    }
    spark.sql("REFRESH TABLE " + table);
  }

  private Dataset openDataset(String table) throws Exception {
    return Dataset.open().allocator(LanceRuntime.allocator()).uri(datasetUriOf(table)).build();
  }

  private String datasetUriOf(String table) throws Exception {
    return lanceTable(table).readOptions().getDatasetUri();
  }

  private LanceDataset lanceTable(String fullName) throws Exception {
    String name = fullName.substring(fullName.lastIndexOf('.') + 1);
    return (LanceDataset)
        ((TableCatalog) spark.sessionState().catalogManager().catalog(CATALOG_NAME))
            .loadTable(Identifier.of(new String[] {"default"}, name));
  }

  // ---------------------------------------------------------------- helpers: query

  /**
   * Runs {@code VECTOR_SEARCH} on the distributed path. {@code nprobes} is an exact probe count per
   * segment: {@code 1} matches a single-partition IVF, and a segment with {@code k} partitions
   * needs {@code nprobes = k} to scan them all. {@code bypass=true} answers from a flat scan of
   * every fragment, which is the exact reference.
   */
  private List<Integer> search(
      String table,
      List<Float> query,
      int k,
      Integer refineFactor,
      boolean bypass,
      Integer nprobes) {
    StringBuilder sql =
        new StringBuilder("SELECT id, _distance FROM VECTOR_SEARCH(table => '")
            .append(table)
            .append("', query_vector => ")
            .append(vectorSql(query))
            .append(", vector_column => '")
            .append(VECTOR_COLUMN)
            .append("', k => ")
            .append(k);
    if (nprobes != null) {
      sql.append(", nprobes => ").append(nprobes);
    }
    if (refineFactor != null) {
      sql.append(", refine_factor => ").append(refineFactor);
    }
    if (bypass) {
      sql.append(", bypass_vector_index => true");
    }
    sql.append(") ORDER BY _distance, id");

    spark.conf().set("spark.sql.lance.search.distributed.enabled", "true");
    return spark.sql(sql.toString()).collectAsList().stream()
        .map(row -> row.getInt(0))
        .collect(Collectors.toList());
  }

  private double meanRecall(String table, int bits, int refineFactor, int k, Integer nprobes) {
    double total = 0.0;
    List<List<Float>> queries = queryVectors();
    for (List<Float> query : queries) {
      List<Integer> exact = search(table, query, k, null, true, 1);
      List<Integer> approx = search(table, query, k, refineFactor, false, nprobes);
      Set<Integer> hit = new HashSet<>(approx);
      hit.retainAll(new HashSet<>(exact));
      total += (double) hit.size() / exact.size();
    }
    return Math.round(total / queries.size() * 1000.0) / 1000.0;
  }

  private static String vectorSql(List<Float> vector) {
    return "array(" + vector.stream().map(v -> v + "f").collect(Collectors.joining(", ")) + ")";
  }

  /** Guards the assumption the recall matrix depends on: the table really holds 8 fragments. */
  @Test
  void tableHasTheExpectedShape() {
    assertEquals(2_000, TOTAL_ROWS);
    assertEquals(0, DIM % 8, "RaBitQ requires a dimension divisible by 8");
  }
}
