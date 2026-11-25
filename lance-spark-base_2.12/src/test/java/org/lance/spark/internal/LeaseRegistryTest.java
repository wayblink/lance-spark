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
package org.lance.spark.internal;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bounds, reference counting and eviction of the process-local dataset lease. The registry is
 * generic so these run on a stub resource: no Lance dataset, no Spark session, no JNI.
 */
class LeaseRegistryTest {
  private static LeaseRegistry.Key key(String pinnedRef) {
    return LeaseRegistry.Key.of("catalog", "identity", "table:1", pinnedRef, "credentials", "v1");
  }

  /** A stub lease value: counts how often it was opened and closed. */
  private static final class Resource implements AutoCloseable {
    private final AtomicInteger closes;
    private final String name;

    Resource(AtomicInteger closes, String name) {
      this.closes = closes;
      this.name = name;
    }

    @Override
    public void close() {
      closes.incrementAndGet();
    }

    @Override
    public String toString() {
      return name;
    }
  }

  @Test
  void aSecondAcquireOfTheSameKeyReusesTheOpenedResource() {
    AtomicInteger opens = new AtomicInteger();
    AtomicInteger closes = new AtomicInteger();
    LeaseRegistry<Resource> registry = new LeaseRegistry<>(8, 60_000, 60_000);

    LeaseRegistry.Handle<Resource> first =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r" + opens.incrementAndGet()));
    assertTrue(first.opened(), "the first acquire has to open");
    assertFalse(first.reused(), "the first acquire cannot reuse anything");
    Resource resource = first.value();
    first.close();
    assertEquals(0, closes.get(), "a released lease stays cached, so it is not closed yet");

    LeaseRegistry.Handle<Resource> second =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r" + opens.incrementAndGet()));
    assertTrue(second.reused(), "the second acquire must reuse the cached open");
    assertFalse(second.opened(), "the second acquire must not open again");
    assertSame(resource, second.value(), "both consumers must see the same open");
    assertEquals(1, opens.get(), "one open for two sequential leases on the same key");
    second.close();
    registry.close();
    assertEquals(1, closes.get(), "closing the registry closes the cached resource once");
  }

  @Test
  void differentPinnedVersionsNeverShareAnOpen() {
    AtomicInteger opens = new AtomicInteger();
    AtomicInteger closes = new AtomicInteger();
    LeaseRegistry<Resource> registry = new LeaseRegistry<>(8, 60_000, 60_000);

    LeaseRegistry.Handle<Resource> oldVersion =
        registry.acquire(
            key("main@7"),
            () -> {
              opens.incrementAndGet();
              return new Resource(closes, "old");
            });
    LeaseRegistry.Handle<Resource> newVersion =
        registry.acquire(
            key("main@8"),
            () -> {
              opens.incrementAndGet();
              return new Resource(closes, "new");
            });
    assertNotSame(
        oldVersion.value(), newVersion.value(), "a new version must not reuse the old one");
    assertTrue(newVersion.opened(), "an unseen version has to be opened");
    assertEquals(2, opens.get(), "two pinned versions, two opens");
    oldVersion.close();
    newVersion.close();
    registry.close();
    assertEquals(2, closes.get());
  }

  @Test
  void theFirstReleaseDoesNotCloseAndTheLastOneDoes() {
    AtomicInteger closes = new AtomicInteger();
    LeaseRegistry<Resource> registry = new LeaseRegistry<>(0, 60_000, 60_000);
    LeaseRegistry.Handle<Resource> first =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r"));
    LeaseRegistry.Handle<Resource> second =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r"));
    assertSame(first.value(), second.value(), "two concurrent consumers share one open");
    first.close();
    assertEquals(0, closes.get(), "the first close must not release a resource still in use");
    second.close();
    assertEquals(1, closes.get(), "the last close releases it");
  }

  @Test
  void aDoubleCloseDoesNotReleaseTwice() {
    AtomicInteger closes = new AtomicInteger();
    LeaseRegistry<Resource> registry = new LeaseRegistry<>(0, 60_000, 60_000);
    LeaseRegistry.Handle<Resource> first =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r"));
    LeaseRegistry.Handle<Resource> second =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r"));
    first.close();
    first.close();
    assertEquals(0, closes.get(), "closing one handle twice must not drop another consumer's ref");
    second.close();
    assertEquals(1, closes.get(), "the surviving consumer still closes it once");
  }

  @Test
  void concurrentAcquiresOfOneKeyOpenExactlyOnce() throws Exception {
    AtomicInteger opens = new AtomicInteger();
    AtomicInteger closes = new AtomicInteger();
    LeaseRegistry<Resource> registry = new LeaseRegistry<>(8, 60_000, 60_000);
    int consumers = 8;
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(consumers);
    try {
      List<Future<LeaseRegistry.Handle<Resource>>> futures = new ArrayList<>();
      for (int i = 0; i < consumers; i++) {
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  return registry.acquire(
                      key("main@7"),
                      () -> {
                        opens.incrementAndGet();
                        sleep(50);
                        return new Resource(closes, "r");
                      });
                }));
      }
      start.countDown();
      Resource expected = null;
      for (Future<LeaseRegistry.Handle<Resource>> future : futures) {
        LeaseRegistry.Handle<Resource> handle = future.get(30, TimeUnit.SECONDS);
        if (expected == null) {
          expected = handle.value();
        }
        assertSame(expected, handle.value(), "every consumer must see one shared open");
        handle.close();
      }
      assertEquals(1, opens.get(), "eight concurrent consumers must open once, got " + opens.get());
    } finally {
      pool.shutdownNow();
      registry.close();
    }
  }

  @Test
  void anExpiredLeaseIsReopenedInsteadOfReused() throws Exception {
    AtomicInteger opens = new AtomicInteger();
    AtomicInteger closes = new AtomicInteger();
    LeaseRegistry<Resource> registry = new LeaseRegistry<>(8, 40, 60_000);
    LeaseRegistry.Handle<Resource> first =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r"));
    first.close();
    Thread.sleep(80);
    LeaseRegistry.Handle<Resource> second =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r"));
    assertTrue(second.opened(), "a lease older than max age must be reopened, not reused");
    assertFalse(second.reused(), "reusing an aged lease would keep a stale namespace client alive");
    assertEquals(1, closes.get(), "the aged entry is closed once when it is replaced");
    second.close();
    registry.close();
    assertEquals(2, closes.get(), "the replacement is closed by the final teardown");
  }

  @Test
  void anIdleLeaseIsEvictedAndReopened() throws Exception {
    AtomicInteger closes = new AtomicInteger();
    LeaseRegistry<Resource> registry = new LeaseRegistry<>(8, 60_000, 40);
    LeaseRegistry.Handle<Resource> first =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r"));
    first.close();
    Thread.sleep(80);
    LeaseRegistry.Handle<Resource> second =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r"));
    assertTrue(second.opened(), "an idle entry must be evicted, so the next acquire opens");
    assertEquals(1, closes.get(), "eviction closes the idle entry");
    second.close();
    registry.close();
  }

  @Test
  void theEntryBoundEvictsTheLeastRecentlyUsed() {
    AtomicInteger closes = new AtomicInteger();
    LeaseRegistry<Resource> registry = new LeaseRegistry<>(2, 60_000, 60_000);
    LeaseRegistry.Handle<Resource> one =
        registry.acquire(key("main@1"), () -> new Resource(closes, "1"));
    LeaseRegistry.Handle<Resource> two =
        registry.acquire(key("main@2"), () -> new Resource(closes, "2"));
    one.close();
    two.close();
    // Touch 1 so that 2 becomes the least recently used.
    registry.acquire(key("main@1"), () -> new Resource(closes, "1")).close();
    registry.acquire(key("main@3"), () -> new Resource(closes, "3")).close();
    assertEquals(1, closes.get(), "the bound evicts exactly one entry, got " + closes.get());
    assertTrue(
        registry.stats().cachedEntries() <= 2, "the bound must hold, got " + registry.stats());
  }

  @Test
  void aFailedOpenLeavesNoPoisonedEntry() {
    AtomicInteger closes = new AtomicInteger();
    LeaseRegistry<Resource> registry = new LeaseRegistry<>(8, 60_000, 60_000);
    assertThrows(
        IllegalStateException.class,
        () ->
            registry.acquire(
                key("main@7"),
                () -> {
                  throw new IllegalStateException("boom");
                }));
    LeaseRegistry.Handle<Resource> retry =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r"));
    assertTrue(retry.opened(), "after a failed open the key must be retryable");
    retry.close();
    registry.close();
    assertEquals(1, closes.get());
  }

  @Test
  void aZeroEntryBoundClosesEveryLeaseLikeTheBaseline() {
    AtomicInteger closes = new AtomicInteger();
    LeaseRegistry<Resource> registry = new LeaseRegistry<>(0, 60_000, 60_000);
    LeaseRegistry.Handle<Resource> first =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r"));
    first.close();
    assertEquals(
        1, closes.get(), "with no caching configured every release closes, as before the lease");
    LeaseRegistry.Handle<Resource> second =
        registry.acquire(key("main@7"), () -> new Resource(closes, "r"));
    assertTrue(second.opened(), "nothing is cached, so the next acquire opens");
    second.close();
    registry.close();
    assertEquals(2, closes.get());
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
