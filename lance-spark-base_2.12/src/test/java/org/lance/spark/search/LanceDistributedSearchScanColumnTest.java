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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers how a named index reconciles the caller's {@code vector_column} with the canonical path
 * the index was built on. Plain JVM: no Spark session, no Lance native library.
 */
class LanceDistributedSearchScanColumnTest {
  @Test
  void acceptsTheCanonicalPath() {
    assertTrue(LanceDistributedSearchScan.sameVectorColumn("vector", "vector"));
    assertTrue(LanceDistributedSearchScan.sameVectorColumn("payload.vec", "payload.vec"));
  }

  @Test
  void acceptsAnUnquotedNameWhoseCanonicalFormIsQuoted() {
    // canonicalPath quotes a part only when it has to, so the same field has two spellings.
    assertTrue(LanceDistributedSearchScan.sameVectorColumn("my-vec", "`my-vec`"));
    assertTrue(LanceDistributedSearchScan.sameVectorColumn("`my-vec`", "`my-vec`"));
    assertTrue(LanceDistributedSearchScan.sameVectorColumn("payload.my-vec", "payload.`my-vec`"));
  }

  @Test
  void rejectsADifferentColumn() {
    assertFalse(LanceDistributedSearchScan.sameVectorColumn("vector", "payload.vec"));
    assertFalse(LanceDistributedSearchScan.sameVectorColumn("vec", "vector"));
  }

  @Test
  void rejectsAMalformedPath() {
    assertFalse(LanceDistributedSearchScan.sameVectorColumn("a..b", "a.b"));
    assertFalse(LanceDistributedSearchScan.sameVectorColumn("", "vector"));
  }
}
