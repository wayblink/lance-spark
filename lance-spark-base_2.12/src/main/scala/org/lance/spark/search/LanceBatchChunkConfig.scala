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
 * Resolved chunking knobs, validated once on the driver so no executor has to interpret a
 * configuration string, and carried to the tasks with the partition.
 */
final case class LanceBatchChunkConfig(
    budgetBytes: Long,
    maxQueriesPerChunk: Int,
    minQueriesPerChunk: Int)
  extends Serializable {

  /** Whether an explicit per-scan query cap was configured. */
  def isCapped: Boolean = maxQueriesPerChunk > 0
}

object LanceBatchChunkConfig {

  def of(
      budgetBytes: Long,
      maxQueriesPerChunk: Int,
      minQueriesPerChunk: Int): LanceBatchChunkConfig = {
    if (budgetBytes <= 0) {
      throw new IllegalArgumentException(
        s"${LanceBatchChunking.BudgetBytesConf} must be positive, but was $budgetBytes")
    }
    if (minQueriesPerChunk <= 0) {
      throw new IllegalArgumentException(
        s"${LanceBatchChunking.MinQueriesConf} must be at least 1, but was $minQueriesPerChunk")
    }
    if (maxQueriesPerChunk < 0) {
      throw new IllegalArgumentException(
        s"${LanceBatchChunking.MaxQueriesConf} must be at least 0 (0 disables the cap), " +
          s"but was $maxQueriesPerChunk")
    }
    if (maxQueriesPerChunk > 0 && maxQueriesPerChunk < minQueriesPerChunk) {
      throw new IllegalArgumentException(
        s"${LanceBatchChunking.MaxQueriesConf} is $maxQueriesPerChunk, below " +
          s"${LanceBatchChunking.MinQueriesConf}=$minQueriesPerChunk; one of the two has to " +
          "change, or every chunk would break one of them")
    }
    LanceBatchChunkConfig(budgetBytes, maxQueriesPerChunk, minQueriesPerChunk)
  }

  def default: LanceBatchChunkConfig =
    LanceBatchChunkConfig(
      LanceBatchChunking.DefaultBudgetBytes,
      LanceBatchChunking.DefaultMaxQueries,
      LanceBatchChunking.DefaultMinQueries)
}
