/* Exact truth only: no ANN index, no TVF benchmark timings, no table mutations. */
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.index.DistanceType;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceRef;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;

public final class TruthGenerator {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP = new TypeReference<Map<String, Object>>() {};
  private static final String FLOAT_HASH = "e60ccd2f1e2193e8d4c0fc25a4cdae7ac2ebe75621d7080a1cad65a888b4d95a";
  private static final String PARQUET_HASH = "dd29359aecf3fdef8de7ba3dbb21743257851eb72c99514b459c7aa15d4c7f63";
  private static final int QUERY_COUNT = 100;
  private static final int DIMENSION = 2560;
  private static final int K = 2000;
  private static final int FRAGMENT_COUNT = 586;

  static void require(boolean condition, String message) {
    if (!condition) throw new IllegalStateException(message);
  }

  static final class Hit implements Serializable {
    private static final long serialVersionUID = 1L;
    final long rowid;
    final String id;
    final float distance;
    Hit(long rowid, String id, float distance) {
      require(rowid >= 0 && id != null && Float.isFinite(distance) && distance >= 0, "invalid exact hit");
      this.rowid = rowid;
      this.id = id;
      this.distance = distance;
    }
  }

  static final class HitOrder implements Comparator<Hit>, Serializable {
    private static final long serialVersionUID = 1L;
    private final boolean reversed;
    HitOrder(boolean reversed) { this.reversed = reversed; }
    @Override public int compare(Hit a, Hit b) {
      int distance = Float.compare(a.distance, b.distance);
      int order = distance != 0 ? distance : Long.compare(a.rowid, b.rowid);
      return reversed ? -order : order;
    }
  }

  /** O(k) retained entities; doc_uuid is payload, never the entity key. */
  static final class TopK implements Serializable {
    private static final long serialVersionUID = 1L;
    final int k;
    final PriorityQueue<Hit> heap;
    final Set<Long> retained = new HashSet<>();
    TopK(int k) {
      require(k > 0, "positive k required");
      this.k = k;
      heap = new PriorityQueue<>(k, new HitOrder(true));
    }
    void add(Hit hit) {
      require(!retained.contains(hit.rowid), "duplicate retained row entity");
      if (heap.size() < k) {
        heap.add(hit); retained.add(hit.rowid);
      } else if (new HitOrder(false).compare(hit, heap.element()) < 0) {
        retained.remove(heap.remove().rowid); heap.add(hit); retained.add(hit.rowid);
      }
    }
    void merge(TopK other) { for (Hit hit : other.heap) add(hit); }
    List<Hit> ordered() {
      List<Hit> out = new ArrayList<>(heap); out.sort(new HitOrder(false)); return out;
    }
  }

  /** One bounded heap per query plus the exact successfully scanned fragment set. */
  static final class Block implements Serializable {
    private static final long serialVersionUID = 1L;
    final TopK[] top;
    final BitSet coverage = new BitSet();
    long scans;
    Block(int queryCount, int k) {
      top = new TopK[queryCount];
      for (int q = 0; q < queryCount; q++) top[q] = new TopK(k);
    }
    Block merge(Block other) {
      BitSet overlap = (BitSet) coverage.clone(); overlap.and(other.coverage);
      require(overlap.isEmpty(), "duplicate fragment coverage");
      for (int q = 0; q < top.length; q++) top[q].merge(other.top[q]);
      coverage.or(other.coverage); scans += other.scans; return this;
    }
  }

  static Map<String, Object> readJson(Path path, Configuration conf) throws Exception {
    try (InputStream in = path.getFileSystem(conf).open(path)) { return JSON.readValue(in, MAP); }
  }
  static boolean jsonEqual(Object left, Object right) {
    return JSON.valueToTree(left).equals(JSON.valueToTree(right));
  }
  static byte[] readBytes(Path path, Configuration conf) throws Exception {
    try (InputStream in = path.getFileSystem(conf).open(path)) { return in.readAllBytes(); }
  }
  static String digest(byte[] bytes) throws Exception {
    byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes);
    StringBuilder out = new StringBuilder();
    for (byte b : hash) out.append(String.format("%02x", b & 255));
    return out.toString();
  }
  static String fileDigest(Path path, Configuration conf) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream in = path.getFileSystem(conf).open(path)) {
      byte[] buffer = new byte[64 * 1024]; int read;
      while ((read = in.read(buffer)) >= 0) if (read > 0) digest.update(buffer, 0, read);
    }
    StringBuilder out = new StringBuilder();
    for (byte b : digest.digest()) out.append(String.format("%02x", b & 255));
    return out.toString();
  }
  static void create(Path path, byte[] bytes, Configuration conf) throws Exception {
    try (java.io.OutputStream out = path.getFileSystem(conf).create(path, false)) { out.write(bytes); }
  }
  static void emit(String kind, Object value) throws Exception {
    System.out.println("TRUTH3271 " + kind + " " + JSON.writeValueAsString(value)); System.out.flush();
  }
  static String fragmentHash(List<Integer> fragments) throws Exception {
    return digest(fragments.stream().sorted().map(String::valueOf).collect(Collectors.joining("\n"))
        .getBytes(StandardCharsets.UTF_8));
  }
  static List<Integer> integerList(Object values) {
    List<Integer> result = new ArrayList<>();
    for (Object value : (List<?>) values) result.add(((Number) value).intValue());
    return result;
  }

  static Map<String, String> storage(Configuration conf) {
    Map<String, String> result = new LinkedHashMap<>();
    for (String[] pair : new String[][] {
        {"endpoint", "AWS_ENDPOINT_URL", "fs.s3a.endpoint"},
        {"access_key_id", "AWS_ACCESS_KEY_ID", "fs.s3a.access.key"},
        {"secret_access_key", "AWS_SECRET_ACCESS_KEY", "fs.s3a.secret.key"},
        {"session_token", "AWS_SESSION_TOKEN", "fs.s3a.session.token"}}) {
      String value = System.getenv(pair[1]);
      if (value == null || value.isEmpty()) value = conf.get(pair[2]);
      if (value != null && !value.isEmpty()) result.put(pair[0], value);
    }
    require(result.containsKey("endpoint") && result.containsKey("access_key_id")
        && result.containsKey("secret_access_key"), "object-store configuration missing");
    result.put("region", System.getenv().getOrDefault("AWS_REGION", "us-east-1"));
    result.put("allow_http", "true"); result.put("virtual_hosted_style_request", "false");
    return result;
  }
  static LanceSparkReadOptions options(String uri, String vectorColumn, Map<String, String> storage) {
    return LanceSparkReadOptions.builder().datasetUri(uri).catalogName("lance")
        .ref(LanceRef.ofMain(56)).storageOptions(storage).build();
  }

  static Block exactFragment(String uri, String vectorColumn, Map<String, String> storage,
      int fragmentId, float[][] queries) throws Exception {
    Block result = new Block(queries.length, K);
    try (Dataset dataset = Utils.openDatasetBuilder(options(uri, vectorColumn, storage)).build()) {
      require(dataset.version() == 56, "worker snapshot changed");
      Query query = new Query.Builder().setColumn(vectorColumn).setKeys(queries).setK(K)
          .setUseIndex(false).setDistanceType(DistanceType.L2).setQueryParallelism(1).build();
      require(!query.getRefineFactor().isPresent(), "truth must not refine ANN candidates");
      ScanOptions scanOptions = new ScanOptions.Builder().nearest(query)
          .fragmentIds(Collections.singletonList(fragmentId)).prefilter(true)
          .columns(Collections.singletonList("doc_uuid")).withRowId(true).collectStats(true).build();
      try (LanceScanner scanner = dataset.newScan(scanOptions); ArrowReader reader = scanner.scanBatches()) {
        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          require(root.getVector("query_index") != null && root.getVector("_rowid") != null
              && root.getVector("_distance") != null && root.getVector("doc_uuid") != null,
              "native exact result lost identity fields");
          for (int row = 0; row < root.getRowCount(); row++) {
            int q = ((Number) root.getVector("query_index").getObject(row)).intValue();
            require(q >= 0 && q < queries.length, "native query attribution out of range");
            Object id = root.getVector("doc_uuid").getObject(row);
            result.top[q].add(new Hit(((Number) root.getVector("_rowid").getObject(row)).longValue(),
                id == null ? null : id.toString(),
                ((Number) root.getVector("_distance").getObject(row)).floatValue()));
          }
        }
      }
      result.coverage.set(fragmentId); result.scans = 1;
    }
    return result;
  }

  static Map<String, Object> metadata(Map<String, Object> contract) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (String key : new String[] {"dataset_uri", "version", "rows", "vector_column", "dimension", "metric"})
      out.put(key, contract.get(key));
    out.put("query_sha256", FLOAT_HASH); out.put("query_part_sha256", PARQUET_HASH);
    out.put("topk", K); out.put("query_count", QUERY_COUNT); out.put("algorithm", "exact-bypass");
    return out;
  }

  static float[][] loadQueries(SparkSession spark, String parquet) throws Exception {
    List<Row> rows = spark.read().parquet(parquet).select("qid", "vector").orderBy("qid").collectAsList();
    require(rows.size() == QUERY_COUNT, "exactly 100 query vectors required");
    float[][] queries = new float[QUERY_COUNT][DIMENSION];
    MessageDigest hash = MessageDigest.getInstance("SHA-256");
    for (int q = 0; q < QUERY_COUNT; q++) {
      require(((Number) rows.get(q).get(0)).intValue() == q, "qid must be exactly 0..99");
      List<?> vector = rows.get(q).getList(1);
      require(vector != null && vector.size() == DIMENSION, "query dimension mismatch");
      ByteBuffer bytes = ByteBuffer.allocate(12 + DIMENSION * 4).order(ByteOrder.LITTLE_ENDIAN);
      bytes.putLong(q).putInt(DIMENSION);
      for (int d = 0; d < DIMENSION; d++) {
        require(vector.get(d) instanceof Number, "NULL/non-numeric query coordinate");
        float value = ((Number) vector.get(d)).floatValue();
        require(Float.isFinite(value), "non-finite float32 query coordinate");
        queries[q][d] = value; bytes.putFloat(value);
      }
      hash.update(bytes.array());
    }
    StringBuilder canonical = new StringBuilder();
    for (byte b : hash.digest()) canonical.append(String.format("%02x", b & 255));
    require(FLOAT_HASH.equals(canonical.toString()), "canonical float32 query hash mismatch");
    return queries;
  }

  static void verifyExisting(Path blockPath, Path checksumPath, Map<String, Object> expected,
      List<Integer> fragments, int start, int end, Configuration conf) throws Exception {
    require(blockPath.getFileSystem(conf).exists(checksumPath), "partial block without checksum: use a new run directory");
    require(new String(readBytes(checksumPath, conf), StandardCharsets.UTF_8).trim()
        .equals(fileDigest(blockPath, conf)), "resumed block checksum mismatch");
    Map<String, Object> block = readJson(blockPath, conf);
    require("complete".equals(block.get("status")) && jsonEqual(expected, block.get("metadata")), "resumed block identity mismatch");
    require(integerList(block.get("fragments")).equals(fragments)
        && fragmentHash(fragments).equals(block.get("fragment_sha256")), "resumed block coverage mismatch");
    List<Integer> qids = new ArrayList<>(); for (int q = start; q < end; q++) qids.add(q);
    require(qids.equals(integerList(block.get("query_ids"))), "resumed block query range mismatch");
    List<?> results = (List<?>) block.get("queries"); require(results.size() == end - start, "resumed block query count mismatch");
    for (int i = 0; i < results.size(); i++) {
      Map<?, ?> q = (Map<?, ?>) results.get(i);
      require(((Number) q.get("qid")).intValue() == start + i, "resumed qid mismatch");
      List<?> ids = (List<?>) q.get("ids"), rowids = (List<?>) q.get("rowids"), distances = (List<?>) q.get("distances");
      require(ids.size() == K && rowids.size() == K && distances.size() == K, "resumed top-k incomplete");
      Set<Long> seen = new HashSet<>(); float previous = -1; long previousId = -1;
      for (int j = 0; j < K; j++) {
        float distance = ((Number) distances.get(j)).floatValue(); long rowid = ((Number) rowids.get(j)).longValue();
        require(ids.get(j) != null && seen.add(rowid) && rowid >= 0 && Float.isFinite(distance)
            && (Float.compare(distance, previous) > 0 || (Float.compare(distance, previous) == 0 && rowid > previousId)),
            "resumed results not unique/ordered");
        previous = distance; previousId = rowid;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    require(args.length == 3 || args.length == 4, "usage: TruthGenerator CONTRACT_URI OUTPUT_URI BLOCK_SIZE [--resume]");
    boolean resume = args.length == 4 && "--resume".equals(args[3]);
    require(args.length == 3 || resume, "unknown option");
    int blockSize = Integer.parseInt(args[2]); require(blockSize == 10 || blockSize == 20, "block size must be 10 or 20");
    SparkSession spark = SparkSession.builder().appName("truth-3271-exact-bypass").getOrCreate();
    JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
    spark.sparkContext().setLogLevel("WARN");
    String attempt = UUID.randomUUID().toString();
    Configuration conf = spark.sparkContext().hadoopConfiguration();
    Path output = new Path(args[1]); FileSystem fs = output.getFileSystem(conf);
    try {
      Map<String, Object> contract = readJson(new Path(args[0]), conf);
      require(((Number) contract.get("version")).longValue() == 56
          && ((Number) contract.get("rows")).longValue() == 10784907
          && ((Number) contract.get("fragments")).intValue() == FRAGMENT_COUNT
          && ((Number) contract.get("dimension")).intValue() == DIMENSION
          && "l2".equals(contract.get("metric")) && FLOAT_HASH.equals(contract.get("query_float32_sha256"))
          && PARQUET_HASH.equals(contract.get("query_part_sha256")), "frozen exact contract changed");
      String uri = (String) contract.get("dataset_uri"), vectorColumn = (String) contract.get("vector_column");
      String queryPart = (String) contract.get("query_part");
      require(PARQUET_HASH.equals(fileDigest(new Path(queryPart), conf)), "original query parquet hash mismatch");
      float[][] queries = loadQueries(spark, queryPart);
      Map<String, String> storage = storage(conf);
      List<Integer> fragments;
      try (Dataset dataset = Utils.openDatasetBuilder(options(uri, vectorColumn, storage)).build()) {
        require(dataset.version() == 56 && dataset.countRows() == 10784907, "driver snapshot/row count changed");
        fragments = dataset.getFragments().stream().map(Fragment::getId).sorted().collect(Collectors.toList());
      }
      require(fragments.size() == FRAGMENT_COUNT && new HashSet<>(fragments).size() == FRAGMENT_COUNT, "full fragment set required");
      Map<String, Object> identity = metadata(contract);
      Map<String, Object> manifest = new LinkedHashMap<>();
      manifest.put("metadata", identity); manifest.put("block_size", blockSize); manifest.put("fragments", fragments);
      manifest.put("fragment_sha256", fragmentHash(fragments));
      Path manifestPath = new Path(output, "run-manifest.json");
      if (fs.exists(output)) {
        require(resume && fs.exists(manifestPath), "output already exists; explicit --resume with matching manifest required");
        require(jsonEqual(manifest, readJson(manifestPath, conf)), "resume run manifest differs");
      } else {
        require(fs.mkdirs(output), "output directory create failed");
        create(manifestPath, JSON.writeValueAsBytes(manifest), conf);
      }
      emit("start", Map.of("version", 56, "queries", QUERY_COUNT, "fragments", fragments.size(), "block_size", blockSize,
          "k", K, "query_hash", FLOAT_HASH, "attempt", attempt, "formal_truth_generated", false));
      for (int start = 0; start < QUERY_COUNT; start += blockSize) {
        int end = Math.min(start + blockSize, QUERY_COUNT);
        String name = String.format("block-%03d-%03d", start, end - 1);
        Path blockPath = new Path(output, name + ".json"), checksum = new Path(output, name + ".sha256");
        if (fs.exists(blockPath) || fs.exists(checksum)) {
          require(resume && fs.exists(blockPath), "partial/existing block: cannot overwrite");
          verifyExisting(blockPath, checksum, identity, fragments, start, end, conf);
          emit("resume_verified", Map.of("block", name)); continue;
        }
        float[][] matrix = java.util.Arrays.copyOfRange(queries, start, end);
        Broadcast<float[][]> batch = context.broadcast(matrix);
        long blockStart = System.nanoTime();
        try {
          JavaRDD<Block> partial = context.parallelize(fragments, fragments.size())
              .map(fragment -> exactFragment(uri, vectorColumn, storage, fragment, batch.value()));
          // Tree combine bounds each shuffled value to blockSize*k hits; never collect all fragment hits.
          Block merged = partial.treeReduce((left, right) -> left.merge(right), 3);
          require(merged.scans == FRAGMENT_COUNT && merged.coverage.cardinality() == FRAGMENT_COUNT,
              "incomplete or duplicate fragment execution");
          for (int fragment : fragments) require(merged.coverage.get(fragment), "missing expected fragment");
          List<Map<String, Object>> results = new ArrayList<>(); List<Integer> qids = new ArrayList<>();
          for (int q = start; q < end; q++) {
            List<Hit> hits = merged.top[q - start].ordered(); require(hits.size() == K, "global query has fewer than 2000 valid rows");
            List<String> ids = new ArrayList<>(); List<Long> rowids = new ArrayList<>(); List<Float> distances = new ArrayList<>();
            for (Hit hit : hits) { ids.add(hit.id); rowids.add(hit.rowid); distances.add(hit.distance); }
            Map<String, Object> result = new LinkedHashMap<>(); result.put("qid", q); result.put("ids", ids);
            result.put("rowids", rowids); result.put("distances", distances); results.add(result); qids.add(q);
          }
          Map<String, Object> block = new LinkedHashMap<>(); block.put("status", "complete"); block.put("metadata", identity);
          block.put("query_ids", qids); block.put("fragments", fragments); block.put("fragment_sha256", fragmentHash(fragments));
          block.put("queries", results); block.put("attempt", attempt);
          byte[] bytes = JSON.writeValueAsBytes(block);
          create(blockPath, bytes, conf); create(checksum, (digest(bytes) + "\n").getBytes(StandardCharsets.UTF_8), conf);
          verifyExisting(blockPath, checksum, identity, fragments, start, end, conf);
          emit("block_complete", Map.of("block", name, "queries", qids.size(), "coverage", FRAGMENT_COUNT,
              "sha256", digest(bytes), "ms", (System.nanoTime() - blockStart) / 1_000_000));
        } finally { batch.destroy(); }
      }
      // Revalidate all durable blocks before creating the final marker; no failure writes complete.
      for (int start = 0; start < QUERY_COUNT; start += blockSize) {
        int end = Math.min(start + blockSize, QUERY_COUNT);
        String name = String.format("block-%03d-%03d", start, end - 1);
        verifyExisting(new Path(output, name + ".json"), new Path(output, name + ".sha256"), identity, fragments, start, end, conf);
      }
      Path complete = new Path(output, "generation.complete.json");
      if (!fs.exists(complete)) create(complete, JSON.writeValueAsBytes(Map.of("status", "complete", "metadata", identity,
          "fragment_sha256", fragmentHash(fragments), "block_size", blockSize, "attempt", attempt)), conf);
      emit("done", Map.of("ok", true, "queries", QUERY_COUNT, "topk", K, "coverage", FRAGMENT_COUNT,
          "output", output.toString(), "assembly_required", true));
    } catch (Throwable failure) {
      // Log only exception type: storage exceptions can contain credential-bearing endpoints.
      emit("failed", Map.of("ok", false, "attempt", attempt, "exception_type", failure.getClass().getName()));
      throw failure;
    } finally { spark.stop(); }
  }
}
