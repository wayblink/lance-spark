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

import org.lance.spark.LanceRef;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.search.LanceFetchPayloadSource;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The lease key is the whole safety argument: it decides which open a task may reuse. Everything
 * that changes what the dataset sees must split it, and a moving reference must not be reused at
 * all. No dataset is opened here.
 */
class DatasetLeaseTest {
  private static LanceSparkReadOptions.Builder options() {
    return LanceSparkReadOptions.builder()
        .datasetUri("s3://bucket/table")
        .tableId(Collections.singletonList("1"))
        .catalogName("cat")
        .ref(LanceRef.ofMain(7))
        .storageOptions(new HashMap<>(Collections.singletonMap("aws_access_key_id", "key-a")));
  }

  private static LeaseRegistry.Key key(LanceSparkReadOptions options, Map<String, String> initial) {
    return DatasetLease.keyOf(options, initial, "dir", Collections.singletonMap("root", "/tmp"));
  }

  @Test
  void theSameVersionAndCredentialsProduceTheSameKey() {
    Map<String, String> initial = Collections.singletonMap("aws_session_token", "token");
    LeaseRegistry.Key first = key(options().build(), initial);
    LeaseRegistry.Key second = key(options().build(), initial);
    assertNotNull(first);
    assertEquals(first, second, "two tasks of one action must share a lease");
  }

  @Test
  void aDifferentPinnedVersionIsADifferentKey() {
    assertNotEquals(
        key(options().ref(LanceRef.ofMain(7)).build(), null),
        key(options().ref(LanceRef.ofMain(8)).build(), null),
        "a new version must never reuse the previous open");
  }

  @Test
  void rotatedCredentialsAreADifferentKey() {
    Map<String, String> first = Collections.singletonMap("aws_session_token", "token-1");
    Map<String, String> second = Collections.singletonMap("aws_session_token", "token-2");
    assertNotEquals(
        key(options().build(), first),
        key(options().build(), second),
        "a refreshed STS token must not reuse a dataset opened with the old one");
  }

  @Test
  void aDifferentNamespaceIdentityIsADifferentKey() {
    Map<String, String> props = Collections.singletonMap("root", "/other");
    assertNotEquals(
        key(options().build(), null),
        DatasetLease.keyOf(options().build(), null, "dir", props),
        "a different namespace client configuration must open its own dataset");
    assertNotEquals(
        DatasetLease.keyOf(options().build(), null, "dir", props),
        DatasetLease.keyOf(options().build(), null, "rest", props),
        "a different namespace implementation must open its own dataset");
  }

  @Test
  void aDifferentTableOrCatalogIsADifferentKey() {
    assertNotEquals(
        key(options().tableId(Collections.singletonList("1")).build(), null),
        key(options().tableId(Collections.singletonList("2")).build(), null),
        "two tables must not share a lease");
    assertNotEquals(
        key(options().catalogName("cat").build(), null),
        key(options().catalogName("other").build(), null),
        "two catalogs have separate sessions and must not share a lease");
  }

  @Test
  void aBranchRefKeepsItsBranchInTheKey() {
    LeaseRegistry.Key main = key(options().ref(LanceRef.ofMain(7)).build(), null);
    LeaseRegistry.Key branch = key(options().ref(LanceRef.ofBranch("dev", 7)).build(), null);
    assertNotEquals(main, branch, "the same version on another branch is another snapshot");
  }

  /**
   * The deferred payload fetch runs in a second job and reopens the dataset; it can only reuse the
   * search task's open if the key it derives is the same. That depends on the exec passing the
   * scan's pinned options into the source: the planner-time options carry no version and are never
   * leased.
   */
  @Test
  void thePayloadFetchUsesTheSearchTasksPinnedKey() {
    Map<String, String> initial = Collections.singletonMap("aws_session_token", "token");
    Map<String, String> namespace = Collections.singletonMap("root", "/tmp");
    LanceFetchPayloadSource source =
        new LanceFetchPayloadSource(
            options().ref(null).build(),
            initial,
            "dir",
            namespace,
            DatasetLeaseSettings.defaults());
    assertNull(
        DatasetLease.keyOf(source.readOptions(), initial, "dir", namespace),
        "the planning options carry no version, so an unpinned fetch must not lease");

    LanceSparkReadOptions pinned = options().ref(LanceRef.ofMain(7)).build();
    assertEquals(
        DatasetLease.keyOf(pinned, initial, "dir", namespace),
        DatasetLease.keyOf(source.withReadOptions(pinned).readOptions(), initial, "dir", namespace),
        "a fetch handed the scan's pin must land on the search task's lease key");
  }

  @Test
  void aMovingReferenceIsNeverReused() {
    assertNull(
        DatasetLease.keyOf(options().ref(null).build(), null, "dir", null),
        "latest must open per consumer: a cached latest would silently read an old snapshot");
    assertNull(
        DatasetLease.keyOf(options().ref(LanceRef.ofMain()).build(), null, "dir", null),
        "main without a version is still latest");
    assertNull(
        DatasetLease.keyOf(options().ref(LanceRef.ofTag("release")).build(), null, "dir", null),
        "a tag can be moved to another version");
  }

  @Test
  void theKeyNeverCarriesTheCredentialItself() {
    LeaseRegistry.Key leaseKey = key(options().build(), null);
    assertNotNull(leaseKey);
    String printed = leaseKey.toString();
    assertEquals(
        false,
        printed.contains("key-a"),
        "a lease key can reach a log line, so it carries only a digest: " + printed);
  }
}
