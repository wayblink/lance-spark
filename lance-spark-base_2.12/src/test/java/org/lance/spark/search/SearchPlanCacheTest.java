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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The driver-side planning cache: immutable per-version metadata only, bounded by entries, and a
 * new version must never answer from an old snapshot.
 */
class SearchPlanCacheTest {
  private static SearchPlanCache.Key key(String ref) {
    return SearchPlanCache.Key.of("catalog.ns.t", ref, "vector=vector;index=none;metric=l2");
  }

  @Test
  void aSecondQueryOnTheSameVersionReusesTheSnapshot() {
    SearchPlanCache<String> cache = new SearchPlanCache<>(8);
    assertNull(cache.get(key("main@7")), "an unknown version must miss");
    cache.put(key("main@7"), "snapshot-7");
    assertSame("snapshot-7", cache.get(key("main@7")), "the same version must reuse the snapshot");
    assertEquals(1, cache.stats().hits());
    assertEquals(1, cache.stats().misses());
  }

  @Test
  void aNewVersionNeverAnswersFromAnOldSnapshot() {
    SearchPlanCache<String> cache = new SearchPlanCache<>(8);
    cache.put(key("main@7"), "snapshot-7");
    assertNull(cache.get(key("main@8")), "a different version is a different key");
    cache.put(key("main@8"), "snapshot-8");
    assertSame("snapshot-8", cache.get(key("main@8")));
    assertSame(
        "snapshot-7", cache.get(key("main@7")), "the old version stays readable while cached");
  }

  @Test
  void differentIndexKnobsNeverShareASnapshot() {
    SearchPlanCache<String> cache = new SearchPlanCache<>(8);
    cache.put(key("main@7"), "with-index");
    assertNull(
        cache.get(
            SearchPlanCache.Key.of(
                "catalog.ns.t", "main@7", "vector=vector;index=none;metric=cosine")),
        "a different requested metric plans different units, so it cannot reuse the snapshot");
  }

  @Test
  void theEntryBoundEvictsTheLeastRecentlyUsedVersion() {
    SearchPlanCache<String> cache = new SearchPlanCache<>(2);
    cache.put(key("main@1"), "one");
    cache.put(key("main@2"), "two");
    cache.get(key("main@1"));
    cache.put(key("main@3"), "three");
    assertEquals(2, cache.size(), "the entry bound must hold, got " + cache.size());
    assertNotNull(cache.get(key("main@3")));
    assertNotNull(cache.get(key("main@1")), "the recently used version survives");
    assertNull(cache.get(key("main@2")), "the least recently used version is evicted");
  }

  @Test
  void aDisabledCacheNeverReusesAnything() {
    SearchPlanCache<String> cache = new SearchPlanCache<>(0);
    cache.put(key("main@7"), "snapshot-7");
    assertNull(cache.get(key("main@7")), "with no entries configured nothing is cached");
    assertEquals(0, cache.size());
  }
}
