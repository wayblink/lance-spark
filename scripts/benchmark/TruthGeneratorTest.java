/* Runs only pure heap/coverage/serialization invariants; never starts Spark or native Lance. */
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

public final class TruthGeneratorTest {
  static void equal(List<TruthGenerator.Hit> actual, List<TruthGenerator.Hit> expected) {
    TruthGenerator.require(actual.size() == expected.size(), "length");
    for (int i = 0; i < actual.size(); i++) {
      TruthGenerator.require(actual.get(i).rowid == expected.get(i).rowid
          && actual.get(i).distance == expected.get(i).distance
          && actual.get(i).id.equals(expected.get(i).id), "different top-k at rank " + i);
    }
  }
  static void concentrated() {
    TruthGenerator.TopK winners = new TruthGenerator.TopK(7), far = new TruthGenerator.TopK(7);
    List<TruthGenerator.Hit> expected = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      TruthGenerator.Hit hit = new TruthGenerator.Hit(i, "duplicate-doc", i);
      winners.add(hit); far.add(new TruthGenerator.Hit(100 + i, "far", 100 + i));
      if (i < 7) expected.add(hit);
    }
    winners.merge(far); equal(winners.ordered(), expected);
  }
  static void ties() {
    TruthGenerator.TopK top = new TruthGenerator.TopK(3);
    for (long id : new long[] {9, 1, 3, 8}) top.add(new TruthGenerator.Hit(id, "same-doc", id == 8 ? 0 : 1));
    TruthGenerator.require(top.ordered().stream().map(h -> h.rowid).toList().equals(Arrays.asList(8L, 1L, 3L)), "tie order");
  }
  static void randomBounded() {
    Random random = new Random(3271); List<TruthGenerator.Hit> all = new ArrayList<>();
    TruthGenerator.TopK merged = new TruthGenerator.TopK(17);
    for (int fragment = 0; fragment < 11; fragment++) {
      TruthGenerator.TopK local = new TruthGenerator.TopK(17);
      for (int row = 0; row < 1000; row++) {
        TruthGenerator.Hit hit = new TruthGenerator.Hit(fragment * 1000L + row, "repeated", random.nextInt(100));
        all.add(hit); local.add(hit);
        TruthGenerator.require(local.heap.size() <= 17 && local.retained.size() <= 17, "unbounded local heap");
      }
      merged.merge(local); TruthGenerator.require(merged.heap.size() <= 17, "unbounded merge heap");
    }
    all.sort(new TruthGenerator.HitOrder(false)); equal(merged.ordered(), all.subList(0, 17));
  }
  static void coverage() {
    TruthGenerator.Block a = new TruthGenerator.Block(2, 3), b = new TruthGenerator.Block(2, 3);
    a.coverage.set(7); a.scans = 1; b.coverage.set(19); b.scans = 1;
    a.merge(b); TruthGenerator.require(a.scans == 2 && a.coverage.cardinality() == 2, "missing coverage");
    boolean refused = false;
    try { a.merge(b); } catch (IllegalStateException expected) { refused = true; }
    TruthGenerator.require(refused, "overlapping coverage accepted");
  }
  static void serialization() throws Exception {
    TruthGenerator.Block a = new TruthGenerator.Block(2, 3);
    a.top[0].add(new TruthGenerator.Hit(1, "d", 1)); a.coverage.set(7); a.scans = 1;
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) { out.writeObject(a); }
    TruthGenerator.Block b;
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      b = (TruthGenerator.Block) in.readObject();
    }
    equal(a.top[0].ordered(), b.top[0].ordered());
    TruthGenerator.require(b.coverage.equals(a.coverage) && b.scans == 1, "serialized coverage lost");
  }
  public static void main(String[] args) throws Exception {
    var pool = Executors.newFixedThreadPool(4);
    List<Callable<Void>> checks = Arrays.asList(() -> { concentrated(); return null; },
        () -> { ties(); return null; }, () -> { randomBounded(); return null; },
        () -> { coverage(); return null; }, () -> { serialization(); return null; });
    try { for (var future : pool.invokeAll(checks)) future.get(); }
    finally { pool.shutdownNow(); }
    System.out.println("5/5 Java truth heap/coverage invariants passed; workers=4; no Spark/native scan");
  }
}
