/* Executes exactly one approved baseline cell. No index build, table write or count-only timing. */
import com.fasterxml.jackson.databind.*;
import java.io.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.*;

public final class BaselineBenchmark {
  static final ObjectMapper JSON = new ObjectMapper();
  static SparkSession spark;
  static final String VCOL="qwen_3_4b_embedding";
  static final String TABLE="lance.smoke.mm_fixed";
  static final long VERSION=56;
  static void require(boolean ok,String why) {if(!ok)throw new IllegalStateException(why);}
  static JsonNode read(String path) throws Exception {
    Path p=new Path(path);FileSystem fs=p.getFileSystem(spark.sparkContext().hadoopConfiguration());
    try(InputStream in=fs.open(p)){return JSON.readTree(in);}
  }
  static String hash(List<Row> qs) throws Exception {
    MessageDigest h=MessageDigest.getInstance("SHA-256");
    for(Row q:qs) {
      List<?> v=q.getList(1);ByteBuffer b=ByteBuffer.allocate(12+4*v.size()).order(ByteOrder.LITTLE_ENDIAN);
      b.putLong(q.getLong(0)).putInt(v.size());for(Object x:v)b.putFloat(((Number)x).floatValue());h.update(b.array());
    }
    StringBuilder out=new StringBuilder();for(byte b:h.digest())out.append(String.format("%02x",b&255));return out.toString();
  }
  static String vector(List<?> v) {
    StringBuilder b=new StringBuilder("array(");for(Object x:v){if(b.length()>6)b.append(',');b.append(Float.toString(((Number)x).floatValue()));}return b.append(')').toString();
  }
  static String sql(String kind,int k,List<?> v) {
    String query=kind.equals("batch")?"query_table => '_baseline_q100', query_id_column => 'qid', query_vector_column => 'vector'":"query_vector => "+vector(v);
    return "SELECT "+(kind.equals("batch")?"query_id, ":"")+"doc_uuid, _rowid, _distance FROM VECTOR_SEARCH(table => '"+TABLE+"', "+query+
        ", vector_column => '"+VCOL+"', index => 'r3-ivf-4b', version => 56, k => "+k+
        ", nprobes => 8, distance_type => 'l2', with_row_id => true)";
  }
  static void emit(BufferedWriter out,Map<String,Object> record) throws Exception {
    String s=JSON.writeValueAsString(record);out.write(s);out.newLine();out.flush();System.out.println("BASESAMPLE "+s);System.out.flush();
  }
  static void sample(BufferedWriter out,String kind,int k,int qid,List<?> vector,boolean warm,int repeat,
                     JsonNode gold,long actionSeconds) throws Exception {
    String statement=sql(kind,k,vector),group="baseline-"+kind+"-"+k+"-"+qid+"-"+warm+"-"+repeat;
    long started=System.currentTimeMillis(),nano=System.nanoTime();
    spark.sparkContext().setJobGroup(group,"fixed dev baseline cell",true);
    ExecutorService watchdog=Executors.newSingleThreadExecutor();
    Future<?> cancel=watchdog.submit(()->{try{Thread.sleep(actionSeconds*1000);spark.sparkContext().cancelJobGroup(group);}catch(InterruptedException ignored){Thread.currentThread().interrupt();}});
    Map<String,Object> rec=new LinkedHashMap<>();rec.put("kind",kind);rec.put("k",k);rec.put("qid",kind.equals("batch")?"batch":qid);
    rec.put("warmup",warm);rec.put("repeat",repeat);rec.put("started_ms",started);rec.put("sql",statement);
    rec.put("cache_evidence",Map.of("scope","sql-executor-session","residency_verified",false,"status","unknown","reason","b102cc7 SQL metrics omit index cache hits/misses"));
    try {
      // Includes query_table analysis/readQueryTable costs. collect actual row identifiers/distances.
      Dataset<Row> data=spark.sql(statement);List<Row> rows=data.collectAsList();
      double elapsed=(System.nanoTime()-nano)/1_000_000.0;
      int n=kind.equals("batch")?100:1;require(rows.size()==n*k,"result cardinality");
      Map<Integer,List<Map<String,Object>>> answers=new TreeMap<>();Map<Integer,Set<Long>> seen=new HashMap<>();
      for(Row row:rows) {
        int q=kind.equals("batch")?row.getInt(0):qid,base=kind.equals("batch")?1:0;
        require(q>=0&&q<100,"unknown query_id");long id=row.getLong(base+1);float distance=row.getFloat(base+2);
        require(Float.isFinite(distance),"nonfinite distance");require(seen.computeIfAbsent(q,x->new HashSet<>()).add(id),"duplicate rowid");
        answers.computeIfAbsent(q,x->new ArrayList<>()).add(Map.of("doc_uuid",row.getString(base),"_rowid",id,"_distance",distance));
      }
      Map<Integer,Double> recalls=new TreeMap<>();
      for(Map.Entry<Integer,List<Map<String,Object>>> e:answers.entrySet()) {
        require(e.getValue().size()==k,"per-query cardinality");
        e.getValue().sort(Comparator.comparingDouble((Map<String,Object>x)->((Number)x.get("_distance")).doubleValue()).thenComparingLong(x->((Number)x.get("_rowid")).longValue()));
        Set<Long> truth=new HashSet<>();JsonNode exact=gold.get(e.getKey());for(int i=0;i<k;i++)truth.add(exact.get("rowids").get(i).asLong());
        int hits=0;for(long id:seen.get(e.getKey()))if(truth.contains(id))hits++;recalls.put(e.getKey(),hits/(double)k);
      }
      rec.put("ms",elapsed);rec.put("rows",rows.size());rec.put("complete",true);rec.put("results",answers);
      rec.put("recall",recalls);rec.put("plan",data.queryExecution().executedPlan().toString());
      rec.put("timing","spark.sql through collectAsList; validation/JSON excluded");
      emit(out,rec);
    } catch(Throwable t) {
      rec.put("complete",false);rec.put("ms",(System.nanoTime()-nano)/1_000_000.0);rec.put("error_type",t.getClass().getName());emit(out,rec);throw t;
    } finally {cancel.cancel(true);watchdog.shutdownNow();spark.sparkContext().clearJobGroup();}
  }
  public static void main(String[] args) throws Exception {
    require(args.length==6,"BaselineBenchmark contract queries truth output kind k");
    String kind=args[4];int k=Integer.parseInt(args[5]);
    require((kind.equals("single")&&(k==10||k==100||k==2000))||(kind.equals("batch")&&(k==100||k==2000)),"unknown fixed cell");
    spark=SparkSession.builder().appName("lance-dev-baseline-"+kind+"-"+k).getOrCreate();
    try {
      JsonNode cfg=read(args[0]),truth=read(args[2]);
      spark.read().parquet(args[1]).createOrReplaceTempView("_baseline_q100");
      List<Row> qs=spark.sql("SELECT qid,vector FROM _baseline_q100 ORDER BY qid").collectAsList();
      require(qs.size()==100,"need 100 query vectors");for(int i=0;i<100;i++)require(qs.get(i).getLong(0)==i&&qs.get(i).getList(1).size()==2560,"query identity");
      String queryHash=hash(qs);require(queryHash.equals(cfg.get("query_float32_sha256").asText()),"float32 query hash mismatch");
      JsonNode meta=truth.get("metadata");require(meta.get("version").asLong()==VERSION&&meta.get("query_sha256").asText().equals(queryHash)&&meta.get("topk").asInt()==2000,"truth identity");
      JsonNode gold=truth.get("queries");require(gold.size()==100,"truth missing queries");
      for(int i=0;i<100;i++)require(gold.get(i).get("qid").asInt()==i&&gold.get(i).get("rowids").size()==2000,"full truth depth");
      Path output=new Path(args[3]);FileSystem fs=output.getFileSystem(spark.sparkContext().hadoopConfiguration());
      try(BufferedWriter out=new BufferedWriter(new OutputStreamWriter(fs.create(output,false),StandardCharsets.UTF_8))) {
        long seconds=cfg.get("timeouts").get("action_seconds").asLong();
        if(kind.equals("batch")) {
          sample(out,kind,k,0,null,true,-1,gold,seconds);
          for(int r=0;r<3;r++)sample(out,kind,k,0,null,false,r,gold,seconds);
        } else for(int q=0;q<10;q++) {
          List<?> v=qs.get(q).getList(1);sample(out,kind,k,q,v,true,-1,gold,seconds);
          for(int r=0;r<3;r++)sample(out,kind,k,q,v,false,r,gold,seconds);
        }
      }
      System.out.println("BASELINE_DONE {\"ok\":true,\"kind\":\""+kind+"\",\"k\":"+k+"}");
    } finally {spark.catalog().dropTempView("_baseline_q100");spark.stop();}
  }
}
