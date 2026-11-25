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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The chunk plan is the whole rule, so it is tested as arithmetic: no dataset, no Spark session.
 * The rule decides how many queries one native scan may carry, and every query must appear in
 * exactly one chunk in input order - that is what keeps {@code query_id} equal to the input
 * position.
 */
class LanceBatchChunkingTest {
  /** A row a native per-unit scan returns: query_index, id, _distance, _rowid (4 + 4 + 4 + 8). */
  private static final long ROW_WIDTH = 20L;

  private static LanceBatchChunkConfig config(long budgetBytes) {
    return LanceBatchChunkConfig.of(budgetBytes, 0, 1);
  }

  @Test
  void oneChunkWhenTheWholeBatchFits() {
    // per query: 8 floats of keys (32 B) + 3 rows (60 B) = 92 B, so 100 queries are 9200 B.
    LanceBatchChunkPlan plan =
        LanceBatchChunking.plan(100, 8, 3, ROW_WIDTH, config(64L * 1024 * 1024));

    assertEquals(1, plan.chunks(), "a batch inside the budget must run as one native scan");
    assertEquals(100, plan.chunkQueries());
    assertEquals(100, plan.queriesInChunk(0));
    assertEquals(9200L, plan.estimatedBytesPerChunk());
    assertFalse(plan.exceededBudget());
  }

  @Test
  void splitsIntoChunksThatEachFitTheBudget() {
    // per query 32 + 3 * 20 = 92 B; an 920 B budget therefore admits exactly 10 queries.
    LanceBatchChunkPlan plan = LanceBatchChunking.plan(100, 8, 3, ROW_WIDTH, config(920));

    assertEquals(10, plan.chunkQueries());
    assertEquals(10, plan.chunks());
    for (int chunk = 0; chunk < plan.chunks(); chunk++) {
      assertEquals(10, plan.queriesInChunk(chunk));
      assertTrue(
          plan.estimatedBytesOfChunk(chunk) <= 920L,
          "chunk "
              + chunk
              + " must stay inside the budget, was "
              + plan.estimatedBytesOfChunk(chunk));
    }
    assertFalse(plan.exceededBudget());
  }

  @Test
  void everyQueryLandsInExactlyOneChunkInInputOrder() {
    LanceBatchChunkPlan plan = LanceBatchChunking.plan(25, 8, 3, ROW_WIDTH, config(920));

    List<Integer> covered = new ArrayList<>();
    int expectedBase = 0;
    for (int chunk = 0; chunk < plan.chunks(); chunk++) {
      assertEquals(expectedBase, plan.baseOf(chunk), "chunks must be contiguous");
      expectedBase += plan.queriesInChunk(chunk);
      for (int offset = 0; offset < plan.queriesInChunk(chunk); offset++) {
        covered.add(plan.baseOf(chunk) + offset);
      }
    }
    assertEquals(25, covered.size(), "every query must be planned once");
    for (int query = 0; query < 25; query++) {
      assertEquals(Integer.valueOf(query), covered.get(query), "query " + query + " order");
    }
    assertEquals(3, plan.chunks(), "25 queries at 10 per chunk need three chunks");
    assertEquals(5, plan.queriesInChunk(2), "the last chunk holds the remainder");
    assertEquals(460L, plan.estimatedBytesOfChunk(2), "5 queries of 92 B in the tail chunk");
  }

  @Test
  void topKMakesRowsRatherThanKeysTheDominantCost() {
    // per query: 4 floats (16 B) + 1000 rows of 20 B = 20016 B; the budget admits exactly 8.
    LanceBatchChunkPlan plan = LanceBatchChunking.plan(50, 4, 1000, ROW_WIDTH, config(8L * 20016));

    assertEquals(8, plan.chunkQueries());
    assertEquals(7, plan.chunks(), "50 queries at 8 per chunk need seven chunks");
    assertEquals(2, plan.queriesInChunk(6));
  }

  @Test
  void aFloorOnTheChunkSizeIsReportedInsteadOfSilentlyOverrunningTheBudget() {
    // One byte per chunk admits no query at all; the floor of 8 wins, and the plan says so.
    LanceBatchChunkPlan plan =
        LanceBatchChunking.plan(20, 8, 3, ROW_WIDTH, LanceBatchChunkConfig.of(1L, 0, 8));

    assertEquals(8, plan.chunkQueries(), "the minimum chunk size must survive a tiny budget");
    assertEquals(3, plan.chunks());
    assertTrue(plan.exceededBudget(), "a chunk above the budget must be reported, not hidden");
    assertTrue(
        plan.estimatedBytesPerChunk() > 1L,
        "the reported size must show the overrun it warns about");
  }

  @Test
  void anExplicitQueryCapBoundsTheChunkEvenWithRoomToSpare() {
    LanceBatchChunkPlan plan =
        LanceBatchChunking.plan(
            20, 8, 3, ROW_WIDTH, LanceBatchChunkConfig.of(64L * 1024 * 1024, 5, 1));

    assertEquals(5, plan.chunkQueries(), "the configured cap must win over a generous budget");
    assertEquals(4, plan.chunks());
  }

  @Test
  void aSingleQueryIsOneChunk() {
    LanceBatchChunkPlan plan = LanceBatchChunking.plan(1, 8, 3, ROW_WIDTH, config(1L));

    assertEquals(1, plan.chunks());
    assertEquals(1, plan.chunkQueries());
    assertEquals(92L, plan.estimatedBytesOfChunk(0));
  }

  @Test
  void anEmptyBatchPlansNoChunks() {
    LanceBatchChunkPlan plan = LanceBatchChunking.plan(0, 8, 3, ROW_WIDTH, config(1024));

    assertEquals(0, plan.chunks());
    assertEquals(0, plan.chunkQueries());
  }

  @Test
  void theKeysMatrixSizeIsReportedForEveryQueryAndDimension() {
    // This is the part chunking cannot shrink: every task needs every query vector.
    LanceBatchChunkPlan plan = LanceBatchChunking.plan(100, 8, 3, ROW_WIDTH, config(920));

    assertEquals(3200L, plan.estimatedMatrixBytes(), "100 queries of 8 floats are 3200 B");
  }

  @Test
  void configRejectsACapBelowTheMinimum() {
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> LanceBatchChunkConfig.of(1024L, 2, 8));
    assertTrue(
        error.getMessage().contains("chunk.queries"),
        "the error must name the option, got: " + error.getMessage());
  }

  @Test
  void configRejectsANonPositiveBudget() {
    assertThrows(IllegalArgumentException.class, () -> LanceBatchChunkConfig.of(0L, 0, 1));
    assertThrows(IllegalArgumentException.class, () -> LanceBatchChunkConfig.of(-1L, 0, 1));
  }
}
