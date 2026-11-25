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
package org.lance.spark.search

/**
 * How a batch of `queryCount` queries is cut into native scans, decided on the driver and carried
 * unchanged to every task of the scan.
 *
 * <p>Chunk `i` holds queries `[baseOf(i), baseOf(i) + queriesInChunk(i))`: contiguous, covering the
 * batch once, in input order. The reader adds `baseOf(i)` to the `query_index` lance-core reports
 * for chunk `i`, which is what keeps `query_id` equal to the input position.
 */
final case class LanceBatchChunkPlan(
    queryCount: Int,
    chunkQueries: Int,
    estimatedBytesPerChunk: Long,
    estimatedMatrixBytes: Long,
    exceededBudget: Boolean,
    perQueryBytes: Long)
  extends Serializable {

  def chunks(): Int =
    if (queryCount <= 0 || chunkQueries <= 0) 0
    else (queryCount + chunkQueries - 1) / chunkQueries

  def baseOf(chunk: Int): Int = chunk * chunkQueries

  def queriesInChunk(chunk: Int): Int =
    math.max(0, math.min(chunkQueries, queryCount - baseOf(chunk)))

  /** Estimated bytes of one chunk: the keys per query are fixed, only the row count varies. */
  def estimatedBytesOfChunk(chunk: Int): Long =
    perQueryBytes * math.max(queriesInChunk(chunk), 0)

  /** True when the whole batch runs as one native scan, so chunking changes nothing at all. */
  def isSingleChunk: Boolean = chunks() <= 1

  def describe: String =
    s"queries=$queryCount, chunkQueries=$chunkQueries, chunks=${chunks()}, " +
      s"estimatedBytesPerChunk=$estimatedBytesPerChunk, " +
      s"estimatedMatrixBytes=$estimatedMatrixBytes, exceededBudget=$exceededBudget"
}

object LanceBatchChunkPlan {

  /** A plan for a search that is not a batch: one native scan, no query attribution. */
  def singleQuery: LanceBatchChunkPlan =
    LanceBatchChunkPlan(1, 1, 0L, 0L, exceededBudget = false, 0L)
}
