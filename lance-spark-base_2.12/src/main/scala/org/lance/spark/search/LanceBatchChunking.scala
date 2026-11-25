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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import java.util.Locale

/**
 * How many queries of a batch one native scan may carry.
 *
 * <p>`query_id` is the position of a query in the input sequence, and chunking is an execution
 * detail that never renumbers a query: [[LanceBatchChunkPlan]] hands the reader contiguous query
 * ranges in input order, and the reader shifts lance-core's per-scan `query_index` by the chunk's
 * base, so a chunked batch answers exactly what an unchunked one answers.
 *
 * <p>Why chunk at all: a batch search hands every query vector to lance-core at once
 * (`Q.setKeys(queries x dimension)`) and gets `queries x k` rows back per search unit. Neither the
 * native scratch nor the Arrow result is bounded by anything below `queries x k`, so a large batch
 * multiplies what one query costs by the batch size - on the same executor, for every unit.
 *
 * <p>Why inside the task rather than in the plan: splitting into more Spark tasks would open the
 * dataset and its index once per task, and peak memory would not drop, because the extra tasks run
 * concurrently. Chunking inside the task keeps one dataset open per task and bounds what is in
 * flight: at most `units x chunkQueries x k` rows over the scan instead of
 * `units x queries x k`.
 *
 * <p>The budget is an estimate, not an allocator: a Spark `ArrayType` does not carry a
 * fixed-size-list length, so a payload column is charged a nominal width. The plan states the
 * numbers it used and [[LanceBatchChunkMetrics]] reports the volume actually submitted, so a
 * benchmark sees the working set it asked for instead of inferring it.
 */
object LanceBatchChunking {

  /** Per native scan working-set budget: query keys plus the `queries x k` rows they return. */
  val BudgetBytesConf = "spark.sql.lance.search.batch.chunk.bytes"

  /** Hard cap on queries per native scan, whatever the budget allows. `0` disables the cap. */
  val MaxQueriesConf = "spark.sql.lance.search.batch.chunk.queries"

  /**
   * Floor on queries per native scan. A budget so small that one query already exceeds it would
   * otherwise open a scan per query, and the per-scan index lookup would cost more than the memory
   * it saves; the floor wins and the plan reports that it did.
   */
  val MinQueriesConf = "spark.sql.lance.search.batch.min.queries"

  /** 64 MiB: above one 100-query batch of a 1536 dimensional table at k = 100. */
  val DefaultBudgetBytes: Long = 64L * 1024 * 1024

  val DefaultMaxQueries: Int = 0
  val DefaultMinQueries: Int = 1

  private val BytesPerFloat = 4L

  /**
   * Cut `queryCount` queries into native scans of at most `budgetBytes` each.
   *
   * @param dimension floats per query vector
   * @param topK rows one query asks the native scan for
   * @param rowWidth estimated bytes of one result row, from the scan's own schema
   */
  def plan(
      queryCount: Int,
      dimension: Int,
      topK: Int,
      rowWidth: Long,
      config: LanceBatchChunkConfig): LanceBatchChunkPlan = {
    val perQueryBytes =
      math.max(
        1L,
        math.max(dimension, 0) * BytesPerFloat + math.max(topK, 0) * math.max(rowWidth, 0L))
    val matrixBytes =
      if (queryCount <= 0 || dimension <= 0) 0L
      else safeMultiply(queryCount.toLong * dimension.toLong, BytesPerFloat)
    if (queryCount <= 0) {
      return LanceBatchChunkPlan(0, 0, 0L, matrixBytes, exceededBudget = false, perQueryBytes)
    }

    val byBudget = math.max(1L, config.budgetBytes / perQueryBytes)
    val capped =
      if (config.isCapped) math.min(byBudget, config.maxQueriesPerChunk.toLong) else byBudget
    val floored = math.max(capped, config.minQueriesPerChunk.toLong)
    val chunkQueries = math.min(floored, queryCount.toLong).toInt
    val bytesPerChunk = safeMultiply(chunkQueries.toLong, perQueryBytes)
    LanceBatchChunkPlan(
      queryCount,
      chunkQueries,
      bytesPerChunk,
      matrixBytes,
      exceededBudget = bytesPerChunk > config.budgetBytes,
      perQueryBytes)
  }

  /** Estimated width of one result row, from Spark types only; see [[LancePayloadDeferral]]. */
  def estimatedRowWidth(schema: StructType): Long = {
    if (schema == null) return 0L
    schema.fields.foldLeft(0L) { (total, field) =>
      total + LancePayloadDeferral.estimatedWidth(field.dataType)
    }
  }

  /**
   * The chunking knobs of the active session, or the defaults when there is none (plan-only code
   * paths such as a scan built directly by a unit test).
   */
  def fromActiveSession(): LanceBatchChunkConfig =
    SparkSession.getActiveSession match {
      case Some(spark) =>
        LanceBatchChunkConfig.of(
          readLong(spark, BudgetBytesConf, DefaultBudgetBytes),
          readInt(spark, MaxQueriesConf, DefaultMaxQueries),
          readInt(spark, MinQueriesConf, DefaultMinQueries))
      case None => LanceBatchChunkConfig.default
    }

  private def readLong(spark: SparkSession, key: String, default: Long): Long = {
    val raw = read(spark, key, default.toString)
    try {
      raw.toLong
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$key must be a number of bytes, but was '$raw'")
    }
  }

  private def readInt(spark: SparkSession, key: String, default: Int): Int = {
    val raw = read(spark, key, default.toString)
    try {
      raw.toLowerCase(Locale.ROOT) match {
        case "" => default
        case value => value.toInt
      }
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$key must be an integer, but was '$raw'")
    }
  }

  /** A Spark conf read that treats an unset or unparseable option as absent, then validates. */
  private def read(spark: SparkSession, key: String, default: String): String =
    try {
      spark.conf.get(key, default).trim
    } catch {
      case _: RuntimeException => default
    }

  private def safeMultiply(left: Long, right: Long): Long =
    if (left <= 0 || right <= 0) 0L
    else if (left > Long.MaxValue / right) Long.MaxValue
    else left * right
}
