/* Correctness-only pilot. Never mix its explicit projection with the five main baseline cells. */
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.index.DistanceType;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.lance.ipc.ScanStats;
import org.lance.spark.LanceRef;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;

public class BaselinePilot {
  static final ObjectMapper JSON = new ObjectMapper();
  static final String URI = "s3://step5p0-data/wanganyang/iceberg-test/full/lance-dir/dfdfd9b0_smoke$mm_fixed";
  static final String VC = "qwen_3_4b_embedding";
  static final String TABLE = "lance.smoke.mm_fixed";
  static final long VERSION = 56;
  static void emit(String kind, Object record) throws Exception {
    System.out.println("BASEPILOT " + kind + " " + JSON.writeValueAsString(record));
    System.out.flush();
  }
  static final class Hit {
    int query; long rowid; String id; float distance;
    Hit(int q, long r, String i, float d) { query=q; rowid=r; id=i; distance=d; }
  }
  static final Comparator<Hit> ORDER = Comparator.comparingDouble((Hit x)->x.distance)
      .thenComparingLong(x->x.rowid);
  static List<List<Hit>> nativeExact(Dataset ds, float[][] vectors, List<Integer> fragments, int k)
      throws Exception {
    Query query = new Query.Builder().setColumn(VC).setKeys(vectors).setK(k)
        .setUseIndex(false).setDistanceType(DistanceType.L2).build();
    if (query.getRefineFactor().isPresent()) throw new IllegalStateException("refine must be absent");
    ScanOptions options = new ScanOptions.Builder().nearest(query).fragmentIds(fragments)
        .prefilter(true).columns(Arrays.asList("doc_uuid")).withRowId(true).collectStats(true).build();
    List<List<Hit>> out = new ArrayList<>();
    for (int q=0;q<vectors.length;q++) out.add(new ArrayList<>());
    try (org.lance.ipc.LanceScanner scanner=ds.newScan(options);
         ArrowReader reader=scanner.scanBatches()) {
      while(reader.loadNextBatch()) {
        VectorSchemaRoot root=reader.getVectorSchemaRoot();
        for(int i=0;i<root.getRowCount();i++) {
          int q=((Number)root.getVector("query_index").getObject(i)).intValue();
          out.get(q).add(new Hit(q, ((Number)root.getVector("_rowid").getObject(i)).longValue(),
              root.getVector("doc_uuid").getObject(i).toString(),
              ((Number)root.getVector("_distance").getObject(i)).floatValue()));
        }
      }
      Optional<ScanStats> stats=scanner.getStats();
      if(stats.isPresent()) {
        ScanStats s=stats.get(); Map<String,Object> m=new LinkedHashMap<>();
        m.put("fragments",fragments);m.put("bytes",s.getBytesRead());m.put("parts",s.getPartsLoaded());
        m.put("cache_hits",s.getIndexCacheHits());m.put("cache_misses",s.getIndexCacheMisses());
        m.put("counts",s.getAllCounts());emit("native_exact_stats",m);
      }
    }
    for(List<Hit> hits:out) hits.sort(ORDER);
    return out;
  }
  static String argsBase(int k) {
    return "vector_column => '"+VC+"', index => 'r3-ivf-4b', version => 56, k => "+k+
        ", nprobes => 8, distance_type => 'l2', columns => array('doc_uuid'), with_row_id => true";
  }
  static String array(List<?> v) {
    return "array("+v.stream().map(x->Float.toString(((Number)x).floatValue()))
        .collect(Collectors.joining(","))+")";
  }
  public static void main(String[] args) throws Exception {
    if(args.length!=1) throw new IllegalArgumentException("query parquet path required");
    SparkSession spark=SparkSession.builder().appName("baseline-3255-correctness-only").getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    try {
      spark.read().parquet(args[0]).orderBy("qid").limit(3).createOrReplaceTempView("_baseline_pilot_q3");
      List<Row> queries=spark.sql("SELECT qid,vector FROM _baseline_pilot_q3 ORDER BY qid").collectAsList();
      if(queries.size()!=3) throw new IllegalStateException("need three queries");
      List<List<String>> singles=new ArrayList<>();
      float[][] matrix=new float[3][2560];
      for(int q=0;q<queries.size();q++) {
        List<?> vector=queries.get(q).getList(1);
        if(vector.size()!=2560||queries.get(q).getLong(0)!=q) throw new IllegalStateException("query contract");
        for(int d=0;d<2560;d++) matrix[q][d]=((Number)vector.get(d)).floatValue();
        String sql="SELECT doc_uuid,_rowid,_distance FROM VECTOR_SEARCH(table => '"+TABLE+
            "', query_vector => "+array(vector)+", "+argsBase(10)+") ORDER BY _distance,_rowid";
        long t=System.nanoTime();List<Row> hits=spark.sql(sql).collectAsList();
        if(hits.size()!=10) throw new IllegalStateException("single k mismatch");
        List<String> ids=new ArrayList<>();for(Row h:hits) ids.add(h.getString(0)+"/"+h.getLong(1));
        singles.add(ids);emit("single",Map.of("qid",q,"rows",hits.size(),"ids",ids,
            "ms",(System.nanoTime()-t)/1_000_000L,"diagnostic_projection",true));
      }
      String batch="SELECT query_id,doc_uuid,_rowid,_distance FROM VECTOR_SEARCH(table => '"+TABLE+
          "', query_table => '_baseline_pilot_q3', query_id_column => 'qid', query_vector_column => 'vector', "+
          argsBase(10)+") ORDER BY query_id,_distance,_rowid";
      List<Row> batched=spark.sql(batch).collectAsList();
      if(batched.size()!=30) throw new IllegalStateException("batch cardinality");
      for(int q=0;q<3;q++) {
        List<String> ids=new ArrayList<>();for(Row h:batched) if(h.getInt(0)==q) ids.add(h.getString(1)+"/"+h.getLong(2));
        if(!ids.equals(singles.get(q))) throw new IllegalStateException("batch differs query "+q);
      }
      emit("batch_parity",Map.of("ok",true,"queries",3,"rows",batched.size(),"sql",batch));
      Map<String,String> storage=new HashMap<>();
      storage.put("endpoint",System.getenv("AWS_ENDPOINT_URL"));storage.put("access_key_id",System.getenv("AWS_ACCESS_KEY_ID"));
      storage.put("secret_access_key",System.getenv("AWS_SECRET_ACCESS_KEY"));storage.put("region","us-east-1");
      storage.put("allow_http","true");storage.put("virtual_hosted_style_request","false");
      LanceSparkReadOptions opts=LanceSparkReadOptions.builder().datasetUri(URI).catalogName("lance")
          .ref(LanceRef.ofMain(VERSION)).storageOptions(storage).build();
      try(Dataset ds=Utils.openDatasetBuilder(opts).build()) {
        List<Integer> f=ds.getFragments().stream().map(Fragment::getId).sorted().limit(2).collect(Collectors.toList());
        if(ds.version()!=56||f.size()!=2) throw new IllegalStateException("snapshot/fragments mismatch");
        List<List<Hit>> grouped=nativeExact(ds,matrix,f,10);
        List<List<Hit>> merged=new ArrayList<>();for(int q=0;q<3;q++) merged.add(new ArrayList<>());
        for(int fragment:f) {
          List<List<Hit>> local=nativeExact(ds,matrix,Collections.singletonList(fragment),10);
          for(int q=0;q<3;q++) merged.get(q).addAll(local.get(q));
        }
        for(int q=0;q<3;q++) {
          merged.get(q).sort(ORDER);List<Hit> top=merged.get(q).subList(0,10);
          if(grouped.get(q).size()!=10) throw new IllegalStateException("exact cardinality");
          for(int i=0;i<10;i++) {
            Hit a=grouped.get(q).get(i),b=top.get(i);
            if(a.rowid!=b.rowid||!a.id.equals(b.id)||Math.abs(a.distance-b.distance)>1e-5f)
              throw new IllegalStateException("exact merge mismatch q="+q+" rank="+i);
          }
          List<Long> rowids=top.stream().map(x->x.rowid).collect(Collectors.toList());
          List<String> fetched=new ArrayList<>();
          try(ArrowReader r=ds.takeRows(rowids,Collections.singletonList("doc_uuid"))) {
            while(r.loadNextBatch()) {
              VectorSchemaRoot root=r.getVectorSchemaRoot();
              for(int i=0;i<root.getRowCount();i++) fetched.add(root.getVector("doc_uuid").getObject(i).toString());
            }
          }
          if(!fetched.equals(top.stream().map(x->x.id).collect(Collectors.toList())))
            throw new IllegalStateException("takeRows mapping mismatch");
        }
        emit("exact_merge_and_takeRows",Map.of("ok",true,"queries",3,"fragments",f,"k",10,"version",56));
      }
      emit("done",Map.of("ok",true,"correctness_only",true,"formal_truth_generated",false));
    } finally {
      spark.catalog().dropTempView("_baseline_pilot_q3");spark.stop();
    }
  }
}
