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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A bounded, reference-counted lease over an expensive-to-open resource, keyed by everything that
 * decides what that resource sees.
 *
 * <p>The primary user is a Lance {@code Dataset}: opening one reads the manifest and, through the
 * namespace path, re-fetches the table location and vended storage options. Every Spark task used
 * to pay that cost, even when the previous task on the same executor had just opened the same
 * version under the same credentials. A lease keeps such an open alive across tasks and hands it to
 * the next task instead.
 *
 * <p>Rules this registry enforces, because a cached dataset is a stale dataset waiting to happen:
 *
 * <ul>
 *   <li><b>Never across versions.</b> The key carries the pinned dataset version, so a new version
 *       can never answer from an older snapshot; an older version stays readable while cached.
 *   <li><b>Never unauthorised.</b> The key carries a hash of the storage options and of the
 *       namespace identity, so a rotated credential is a different key and forces a fresh open.
 *   <li><b>Bounded age.</b> An entry older than {@code maxAgeMs} is not handed out again: it is
 *       replaced, which rebuilds the namespace client underneath it. This is the bound that keeps
 *       the executor-side credential refresh meaningful.
 *   <li><b>Bounded size.</b> At most {@code maxEntries} idle entries are kept, least recently used
 *       evicted first; with {@code maxEntries == 0} every release closes, which is the pre-lease
 *       behaviour and the switch a deployment turns the lease off with.
 *   <li><b>Reference counted.</b> Concurrent consumers share one open; the first release leaves it
 *       open for the others, the last one either closes it or returns it to the cache.
 *   <li><b>No poisoned slot.</b> A failed open removes its key, so the next consumer retries
 *       instead of joining a permanent failure.
 * </ul>
 *
 * <p>Thread safety: one monitor guards the map and the counters. The resource is opened outside it,
 * so a slow open does not block leases on other keys; concurrent acquires of the same key wait on
 * the entry's latch and then join the open that won.
 */
public final class LeaseRegistry<T extends AutoCloseable> {
  /** How long a consumer waits for another thread's open before opening on its own. */
  private static final long JOIN_TIMEOUT_MS = 120_000L;

  /**
   * How many times an acquire may lose a race, observe a failed open, and retry. Bounded so a
   * pathological opener cannot recurse without limit.
   */
  private static final int MAX_ATTEMPTS = 6;

  /** Opens and closes the resource. The registry never retries it. */
  public interface Opener<T> {
    T open();
  }

  /**
   * Everything that decides which resource an acquisition may reuse. Built by the caller from the
   * read options; the two hash-shaped fields are already digests, so no credential reaches this
   * object in clear text.
   */
  public static final class Key {
    private final String catalog;
    private final String identity;
    private final String resource;
    private final String pinnedRef;
    private final String credentials;
    private final String variant;

    private Key(
        String catalog,
        String identity,
        String resource,
        String pinnedRef,
        String credentials,
        String variant) {
      this.catalog = catalog;
      this.identity = identity;
      this.resource = resource;
      this.pinnedRef = pinnedRef;
      this.credentials = credentials;
      this.variant = variant;
    }

    public static Key of(
        String catalog,
        String identity,
        String resource,
        String pinnedRef,
        String credentials,
        String variant) {
      return new Key(
          normalize(catalog),
          normalize(identity),
          normalize(resource),
          normalize(pinnedRef),
          normalize(credentials),
          normalize(variant));
    }

    private static String normalize(String value) {
      return value == null ? "" : value;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Key)) {
        return false;
      }
      Key that = (Key) other;
      return catalog.equals(that.catalog)
          && identity.equals(that.identity)
          && resource.equals(that.resource)
          && pinnedRef.equals(that.pinnedRef)
          && credentials.equals(that.credentials)
          && variant.equals(that.variant);
    }

    @Override
    public int hashCode() {
      return Objects.hash(catalog, identity, resource, pinnedRef, credentials, variant);
    }

    @Override
    public String toString() {
      // Hashes and identifiers only: this string may reach a log line.
      return "LeaseKey["
          + resource
          + ", "
          + pinnedRef
          + ", identity="
          + identity
          + ", credentials="
          + credentials
          + ", variant="
          + variant
          + "]";
    }
  }

  /** A view of the lease a caller must close. Closing twice is a no-op. */
  public static final class Handle<T extends AutoCloseable> implements AutoCloseable {
    private final T value;
    private final Runnable releaser;
    private final boolean reused;
    private final boolean opened;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    Handle(T value, Runnable releaser, boolean reused, boolean opened) {
      this.value = value;
      this.releaser = releaser;
      this.reused = reused;
      this.opened = opened;
    }

    public T value() {
      return value;
    }

    /** Whether this acquisition reused an open someone else made. */
    public boolean reused() {
      return reused;
    }

    /** Whether this acquisition performed the open itself. */
    public boolean opened() {
      return opened;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        releaser.run();
      }
    }
  }

  /** Counters for tests and for the task metrics the search reader reports. */
  public static final class Stats {
    private final long opens;
    private final long hits;
    private final long evictions;
    private final long closes;
    private final int cachedEntries;

    Stats(long opens, long hits, long evictions, long closes, int cachedEntries) {
      this.opens = opens;
      this.hits = hits;
      this.evictions = evictions;
      this.closes = closes;
      this.cachedEntries = cachedEntries;
    }

    public long opens() {
      return opens;
    }

    public long hits() {
      return hits;
    }

    public long evictions() {
      return evictions;
    }

    public long closes() {
      return closes;
    }

    public int cachedEntries() {
      return cachedEntries;
    }

    @Override
    public String toString() {
      return "LeaseStats[opens="
          + opens
          + ", hits="
          + hits
          + ", evictions="
          + evictions
          + ", closes="
          + closes
          + ", cached="
          + cachedEntries
          + "]";
    }
  }

  private static final class Entry<T> {
    private final Key key;
    private final CountDownLatch ready = new CountDownLatch(1);
    private T value;
    private int refs;
    private long createdAtNanos;
    private long lastUsedNanos;
    private boolean failed;
    private boolean evicted;

    Entry(Key key, long now) {
      this.key = key;
      this.createdAtNanos = now;
      this.lastUsedNanos = now;
    }
  }

  private final Object lock = new Object();
  private final Map<Key, Entry<T>> entries = new HashMap<>();

  private int maxEntries;
  private long maxAgeMs;
  private long idleTimeoutMs;
  private long opens;
  private long hits;
  private long evictions;
  private long closes;
  private boolean closed;

  public LeaseRegistry(int maxEntries, long maxAgeMs, long idleTimeoutMs) {
    this.maxEntries = Math.max(0, maxEntries);
    this.maxAgeMs = Math.max(0L, maxAgeMs);
    this.idleTimeoutMs = Math.max(0L, idleTimeoutMs);
  }

  /**
   * Applies the current configuration. Lowering the entry bound evicts the least recently used idle
   * entries immediately; a zero bound means "no caching", which closes on release and is how the
   * lease is switched off.
   */
  public void configure(int maxEntries, long maxAgeMs, long idleTimeoutMs) {
    List<T> victims = new ArrayList<>();
    synchronized (lock) {
      this.maxEntries = Math.max(0, maxEntries);
      this.maxAgeMs = Math.max(0L, maxAgeMs);
      this.idleTimeoutMs = Math.max(0L, idleTimeoutMs);
      trimLocked(System.nanoTime(), victims);
    }
    closeAll(victims);
  }

  public Handle<T> acquire(Key key, Opener<T> opener) {
    return acquire(key, opener, 0);
  }

  private Handle<T> acquire(Key key, Opener<T> opener, int attempt) {
    if (attempt >= MAX_ATTEMPTS) {
      throw new IllegalStateException(
          "Could not acquire the lease for " + key + " after " + attempt + " attempts");
    }
    List<T> victims = new ArrayList<>();
    Entry<T> owned = null;
    Entry<T> join = null;
    T uncached = null;
    boolean openUncached = false;

    synchronized (lock) {
      if (closed) {
        openUncached = true;
      } else {
        long now = System.nanoTime();
        trimLocked(now, victims);
        Entry<T> entry = entries.get(key);
        if (entry == null) {
          owned = new Entry<>(key, now);
          entries.put(key, owned);
        } else if (entry.value != null) {
          if (!expired(now, entry)) {
            entry.refs++;
            entry.lastUsedNanos = now;
            hits++;
            T value = entry.value;
            return new Handle<>(value, () -> release(entry), true, false);
          }
          if (entry.refs == 0) {
            // Aged out: drop it and open a fresh one, which rebuilds the namespace client.
            entries.remove(key);
            evictLocked(entry, victims);
            owned = new Entry<>(key, now);
            entries.put(key, owned);
          } else {
            // Aged out while another consumer still reads it: serve this one from a private open
            // rather than sharing a lease that is past its freshness bound.
            openUncached = true;
          }
        } else if (entry.failed) {
          entries.remove(key);
          owned = new Entry<>(key, now);
          entries.put(key, owned);
        } else {
          join = entry;
        }
      }
    }
    closeAll(victims);

    if (openUncached) {
      T value = opener.open();
      return new Handle<>(value, () -> closeQuietly(value), false, true);
    }

    if (owned != null) {
      T value;
      try {
        value = opener.open();
      } catch (RuntimeException | Error e) {
        synchronized (lock) {
          owned.failed = true;
          owned.ready.countDown();
          entries.remove(key, owned);
        }
        throw e;
      }
      synchronized (lock) {
        owned.value = value;
        owned.refs = 1;
        owned.createdAtNanos = System.nanoTime();
        owned.lastUsedNanos = owned.createdAtNanos;
        opens++;
        owned.ready.countDown();
      }
      List<T> openedVictims = new ArrayList<>();
      synchronized (lock) {
        trimLocked(System.nanoTime(), openedVictims);
      }
      closeAll(openedVictims);
      Entry<T> entry = owned;
      return new Handle<>(value, () -> release(entry), false, true);
    }

    // Another thread is opening this key: wait for it, then try to join what it opened.
    boolean signalled;
    try {
      signalled = join.ready.await(JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for the lease of " + key, e);
    }
    if (!signalled) {
      // The opener never finished (its thread died, or the open hangs). Forget the placeholder so
      // the next attempt opens on its own instead of waiting forever.
      synchronized (lock) {
        if (entries.get(key) == join) {
          entries.remove(key);
        }
      }
    }
    return acquire(key, opener, attempt + 1);
  }

  private void release(Entry<T> entry) {
    List<T> victims = new ArrayList<>();
    synchronized (lock) {
      if (entry.refs > 0) {
        entry.refs--;
      }
      long now = System.nanoTime();
      entry.lastUsedNanos = now;
      if (entry.refs == 0) {
        boolean keep =
            !closed
                && maxEntries > 0
                && !entry.evicted
                && entry.value != null
                && !expired(now, entry)
                && entries.get(entry.key) == entry;
        if (!keep) {
          entries.remove(entry.key, entry);
          evictLocked(entry, victims);
        }
      }
    }
    closeAll(victims);
  }

  /**
   * Drops entries that are past their freshness bound or idle for too long. Only entries nobody
   * holds: an in-use lease is released by its last consumer, see {@link #release}.
   */
  private void trimLocked(long now, List<T> victims) {
    Iterator<Entry<T>> iterator = entries.values().iterator();
    while (iterator.hasNext()) {
      Entry<T> entry = iterator.next();
      if (entry.refs == 0 && entry.value != null && expired(now, entry)) {
        iterator.remove();
        evictLocked(entry, victims);
      }
    }
    while (entries.size() > maxEntries) {
      Entry<T> leastRecentlyUsed = null;
      for (Entry<T> entry : entries.values()) {
        if (entry.refs == 0 && entry.value != null) {
          if (leastRecentlyUsed == null
              || entry.lastUsedNanos - leastRecentlyUsed.lastUsedNanos < 0) {
            leastRecentlyUsed = entry;
          }
        }
      }
      if (leastRecentlyUsed == null) {
        // Every cached entry is in use. release() closes the ones that would exceed the bound.
        break;
      }
      entries.remove(leastRecentlyUsed.key);
      evictLocked(leastRecentlyUsed, victims);
    }
  }

  private boolean expired(long now, Entry<T> entry) {
    long ageNanos = now - entry.createdAtNanos;
    if (maxAgeMs > 0 && ageNanos > TimeUnit.MILLISECONDS.toNanos(maxAgeMs)) {
      return true;
    }
    return idleTimeoutMs > 0
        && now - entry.lastUsedNanos > TimeUnit.MILLISECONDS.toNanos(idleTimeoutMs);
  }

  private void evictLocked(Entry<T> entry, List<T> victims) {
    entry.evicted = true;
    if (entry.value != null) {
      victims.add(entry.value);
      entry.value = null;
      evictions++;
      closes++;
    }
  }

  private static void closeAll(List<? extends AutoCloseable> victims) {
    for (AutoCloseable victim : victims) {
      closeQuietly(victim);
    }
  }

  private static void closeQuietly(AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception ignored) {
      // A failed close of a cached resource must not fail the task that merely stopped using it.
    }
  }

  public Stats stats() {
    synchronized (lock) {
      int cached = 0;
      for (Entry<T> entry : entries.values()) {
        if (entry.value != null) {
          cached++;
        }
      }
      return new Stats(opens, hits, evictions, closes, cached);
    }
  }

  /** Number of keys currently known to the registry, cached or being opened. */
  public int size() {
    synchronized (lock) {
      return entries.size();
    }
  }

  /** Closes every cached entry; an entry still held is closed by its last release. */
  public void close() {
    List<T> victims = new ArrayList<>();
    synchronized (lock) {
      closed = true;
      Iterator<Entry<T>> iterator = entries.values().iterator();
      while (iterator.hasNext()) {
        Entry<T> entry = iterator.next();
        iterator.remove();
        if (entry.refs == 0) {
          evictLocked(entry, victims);
        } else {
          entry.evicted = true;
        }
      }
    }
    closeAll(victims);
  }

  /**
   * Forgets every cached entry without closing it as an eviction. Only tests use this, so a
   * registry shared by a whole test class does not leak state between cases.
   */
  public void clearForTest() {
    close();
    synchronized (lock) {
      entries.clear();
      closed = false;
      opens = 0;
      hits = 0;
      evictions = 0;
      closes = 0;
    }
  }
}
