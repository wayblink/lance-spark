/* Executes exactly one approved baseline cell: the five vector cells, or the optional text (BM25)
 * cell appended by the text-10m.json contract. No index build, table write or count-only timing. */
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
  static final String TEXT_KIND="text";
  static final int TEXT_K=10;
  static void require(boolean ok,String why) {if(!ok)throw new IllegalStateException(why);}
  static JsonNode read(String path) throws Exception {
    Path p=new Path(path);FileSystem fs=p.getFileSystem(spark.sparkContext().hadoopConfiguration());
    try(InputStream in=fs.open(p)){return JSON.readTree(in);}
  }
  static byte[] readBytes(String path) throws Exception {
    Path p=new Path(path);FileSystem fs=p.getFileSystem(spark.sparkContext().hadoopConfiguration());
    try(InputStream in=fs.open(p)){return in.readAllBytes();}
  }
  static String hex(byte[] bytes) {
    StringBuilder out=new StringBuilder();for(byte b:bytes)out.append(String.format("%02x",b&255));return out.toString();
  }
  static String sha256Hex(byte[] data) throws Exception {
    return hex(MessageDigest.getInstance("SHA-256").digest(data));
  }
  static String hash(List<Row> qs) throws Exception {
    MessageDigest h=MessageDigest.getInstance("SHA-256");
    for(Row q:qs) {
      List<?> v=q.getList(1);ByteBuffer b=ByteBuffer.allocate(12+4*v.size()).order(ByteOrder.LITTLE_ENDIAN);
      b.putLong(q.getLong(0)).putInt(v.size());for(Object x:v)b.putFloat(((Number)x).floatValue());h.update(b.array());
    }
    return hex(h.digest());
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
  static void textSample(BufferedWriter out,int qid,String statement,String query,boolean warm,int repeat,long actionSeconds) throws Exception {
    String group="baseline-text-"+TEXT_K+"-"+qid+"-"+warm+"-"+repeat;
    long started=System.currentTimeMillis(),nano=System.nanoTime();
    spark.sparkContext().setJobGroup(group,"fixed text baseline cell",true);
    ExecutorService watchdog=Executors.newSingleThreadExecutor();
    Future<?> cancel=watchdog.submit(()->{try{Thread.sleep(actionSeconds*1000);spark.sparkContext().cancelJobGroup(group);}catch(InterruptedException ignored){Thread.currentThread().interrupt();}});
    Map<String,Object> rec=new LinkedHashMap<>();rec.put("kind",TEXT_KIND);rec.put("k",TEXT_K);rec.put("qid",qid);
    rec.put("warmup",warm);rec.put("repeat",repeat);rec.put("started_ms",started);rec.put("sql",statement);rec.put("query",query);
    rec.put("cache_evidence",Map.of("scope","sql-executor-session","residency_verified",false,"status","unknown","reason","FTS index cache hits/misses are not exposed by SQL metrics"));
    try {
      Dataset<Row> data=spark.sql(statement);List<Row> rows=data.collectAsList();
      double elapsed=(System.nanoTime()-nano)/1_000_000.0;
      require(rows.size()==TEXT_K,"result cardinality");
      List<Map<String,Object>> results=new ArrayList<>();Set<String> seen=new HashSet<>();
      double previous=Double.POSITIVE_INFINITY;
      for(Row row:rows) {
        String id=row.getString(0);Object rawScore=row.get(1);
        require(id!=null&&!id.isEmpty(),"NULL doc_uuid");require(rawScore instanceof Number,"_score is not numeric");
        double score=((Number)rawScore).doubleValue();require(Double.isFinite(score),"nonfinite _score");
        require(score<=previous,"ORDER BY `_score` DESC not honoured");previous=score;
        require(seen.add(id),"duplicate doc_uuid");results.add(Map.of("doc_uuid",id,"_score",score));
      }
      rec.put("ms",elapsed);rec.put("rows",rows.size());rec.put("complete",true);rec.put("results",results);
      rec.put("plan",data.queryExecution().executedPlan().toString());
      rec.put("timing","spark.sql through collectAsList; validation/JSON excluded");
      emit(out,rec);
    } catch(Throwable t) {
      rec.put("complete",false);rec.put("ms",(System.nanoTime()-nano)/1_000_000.0);rec.put("error_type",t.getClass().getName());emit(out,rec);throw t;
    } finally {cancel.cancel(true);watchdog.shutdownNow();spark.sparkContext().clearJobGroup();}
  }
  static void vectorCell(String[] args,String kind,int k) throws Exception {
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
  }
  static void textCell(String[] args) throws Exception {
    JsonNode cfg=read(args[0]);
    require(TEXT_KIND.equals(cfg.path("kind").asText())&&cfg.path("k").asInt()==TEXT_K,"text contract identity");
    String template=cfg.path("sql_template").asText();
    require(template.contains("{query}"),"text sql template lacks {query}");
    require(template.indexOf("{query}")==template.lastIndexOf("{query}"),"text sql template has several {query}");
    byte[] raw=readBytes(args[1]);
    require(sha256Hex(raw).equals(cfg.path("queries_sha256").asText()),"text query asset hash mismatch");
    int expected=cfg.path("queries").asInt();
    int warmupPasses=cfg.path("warmup_passes").asInt(),repeats=cfg.path("repeats").asInt();
    require(expected>0&&warmupPasses>0&&repeats>0,"text contract counts missing");
    List<String> queries=new ArrayList<>();
    for(String line:new String(raw,StandardCharsets.UTF_8).split("\n",-1)) {
      if(line.endsWith("\r"))line=line.substring(0,line.length()-1);
      if(line.trim().isEmpty())continue;
      JsonNode row=JSON.readTree(line);
      require(row.path("qid").asInt()==queries.size(),"text query qid out of order");
      String query=row.path("query").asText();
      require(!query.isEmpty()&&query.equals(query.trim())&&query.indexOf('\n')<0,"bad text query");
      queries.add(query);
    }
    require(queries.size()==expected,"text query count mismatch");
    Path output=new Path(args[2]);FileSystem fs=output.getFileSystem(spark.sparkContext().hadoopConfiguration());
    long seconds=cfg.path("timeouts").path("action_seconds").asLong();
    require(seconds>0,"text action timeout missing");
    try(BufferedWriter out=new BufferedWriter(new OutputStreamWriter(fs.create(output,false),StandardCharsets.UTF_8))) {
      for(int q=0;q<queries.size();q++) {
        String statement=template.replace("{query}",queries.get(q).replace("'","''"));
        for(int w=0;w<warmupPasses;w++)textSample(out,q,statement,queries.get(q),true,-1,seconds);
        for(int r=0;r<repeats;r++)textSample(out,q,statement,queries.get(q),false,r,seconds);
      }
    }
  }
  public static void main(String[] args) throws Exception {
    require(args.length==5||args.length==6,"BaselineBenchmark contract asset truth output kind k");
    String kind=args[args.length-2];int k=Integer.parseInt(args[args.length-1]);
    boolean text=TEXT_KIND.equals(kind);
    require(text
        ? args.length==5&&k==TEXT_K
        : args.length==6&&((kind.equals("single")&&(k==10||k==100||k==2000))||(kind.equals("batch")&&(k==100||k==2000))),
        "unknown fixed cell");
    spark=SparkSession.builder().appName("lance-dev-baseline-"+kind+"-"+k).getOrCreate();
    try {
      if(text) textCell(args);else vectorCell(args,kind,k);
      System.out.println("BASELINE_DONE {\"ok\":true,\"kind\":\""+kind+"\",\"k\":"+k+"}");
    } finally {spark.catalog().dropTempView("_baseline_q100");spark.stop();}
  }
}
