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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Driver-side cache of immutable, version-scoped search metadata: which fragments exist, which
 * vector index was chosen, which segments cover which fragments, and the metric and column the
 * units were planned with.
 *
 * <p>Index discovery is the expensive half of planning: a listing plus the per-segment statistics
 * of a large vector index, which on a big table is enough to make a driver slow or run it out of
 * memory (see the note on {@code resolveNamedVectorIndex}). All of it is fixed by the dataset
 * version, so a second query on the same version can skip it entirely.
 *
 * <p>What is deliberately <b>not</b> cached: anything that can change without a new version. The
 * key carries the pinned version, so a new commit is a new key; there is no TTL and no "latest"
 * entry, because a stale "latest" is exactly how a query silently reads an older snapshot than the
 * caller asked for.
 *
 * <p>Bounded by entry count, least recently used evicted first; a bound of zero disables caching.
 * Not thread-safe: it is only used by the single driver thread that plans a query, and it is
 * synchronised on itself so that a test (or a future concurrent planner) cannot corrupt it.
 */
public final class SearchPlanCache<V> {
  public static final String ENABLED_CONF = "spark.sql.lance.search.metadata.cache.enabled";
  public static final String ENTRIES_CONF = "spark.sql.lance.search.metadata.cache.entries";
  public static final int DEFAULT_ENTRIES = 8;

  /**
   * Identity of one planning result: the resolved dataset, the pinned version, and every request
   * knob that can select a different index, column or metric.
   */
  public static final class Key {
    private final String resource;
    private final String pinnedRef;
    private final String knobs;

    private Key(String resource, String pinnedRef, String knobs) {
      this.resource = resource;
      this.pinnedRef = pinnedRef;
      this.knobs = knobs;
    }

    /**
     * @param resource catalog, namespace and table, e.g. {@code catalog:table-id}
     * @param pinnedRef the exact snapshot planned on, e.g. {@code main@123}; must not be "latest"
     * @param knobs index name, vector column, distance type and the planning switches
     */
    public static Key of(String resource, String pinnedRef, String knobs) {
      return new Key(
          resource == null ? "" : resource,
          pinnedRef == null ? "" : pinnedRef,
          knobs == null ? "" : knobs);
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
      return resource.equals(that.resource)
          && pinnedRef.equals(that.pinnedRef)
          && knobs.equals(that.knobs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(resource, pinnedRef, knobs);
    }

    @Override
    public String toString() {
      return "SearchPlanKey[" + resource + ", " + pinnedRef + ", " + knobs + "]";
    }
  }

  /** Hit and size counters, reported in the planning log line and asserted by tests. */
  public static final class Stats {
    private final long hits;
    private final long misses;
    private final long evictions;
    private final int entries;

    Stats(long hits, long misses, long evictions, int entries) {
      this.hits = hits;
      this.misses = misses;
      this.evictions = evictions;
      this.entries = entries;
    }

    public long hits() {
      return hits;
    }

    public long misses() {
      return misses;
    }

    public long evictions() {
      return evictions;
    }

    public int entries() {
      return entries;
    }

    @Override
    public String toString() {
      return "SearchPlanStats[hits="
          + hits
          + ", misses="
          + misses
          + ", evictions="
          + evictions
          + ", entries="
          + entries
          + "]";
    }
  }

  private final LinkedHashMap<Key, V> entries =
      new LinkedHashMap<Key, V>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Key, V> eldest) {
          boolean evict = maxEntries <= 0 || size() > maxEntries;
          if (evict) {
            evictions++;
          }
          return evict;
        }
      };

  private int maxEntries;
  private long hits;
  private long misses;
  private long evictions;

  public SearchPlanCache(int maxEntries) {
    this.maxEntries = Math.max(0, maxEntries);
  }

  /** Applies a new entry bound, evicting from the cold end immediately. */
  public synchronized void configure(int maxEntries) {
    this.maxEntries = Math.max(0, maxEntries);
    if (maxEntries <= 0) {
      evictions += entries.size();
      entries.clear();
      return;
    }
    while (entries.size() > maxEntries) {
      Key eldest = entries.keySet().iterator().next();
      entries.remove(eldest);
      evictions++;
    }
  }

  /** The planning result for this key, or null when it has never been planned (or was evicted). */
  public synchronized V get(Key key) {
    V value = entries.get(key);
    if (value == null) {
      misses++;
    } else {
      hits++;
    }
    return value;
  }

  public synchronized void put(Key key, V value) {
    entries.put(key, value);
  }

  public synchronized Stats stats() {
    return new Stats(hits, misses, evictions, entries.size());
  }

  public synchronized int size() {
    return entries.size();
  }

  public synchronized void clear() {
    entries.clear();
    hits = 0;
    misses = 0;
    evictions = 0;
  }
}
