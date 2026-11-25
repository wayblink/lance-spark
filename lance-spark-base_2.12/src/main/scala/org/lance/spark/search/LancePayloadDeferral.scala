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
import org.apache.spark.sql.types._

import java.util.Locale

/**
 * Decides whether a distributed vector search reads its payload inside the native per-unit scan
 * (the fused plan) or defers it to a row-id fetch after the global ranking.
 *
 * <p>The unit of the cost model is a single search task's payload volume: `k x queries x row
 * width`. A distributed plan runs one such task per index segment (or per unindexed fragment), and
 * every task reads its own local top-k with payload even though only `k x queries` rows survive the
 * global ranking, so the fused plan reads that volume `units` times. Deferring removes those extra
 * copies but adds one random-access pass over the winners, chunked so a single Arrow IPC buffer
 * stays bounded.
 *
 * <p>The number of units is not known while the logical plan is built (it needs index discovery),
 * so the threshold is stated per unit and the mode is explicit:
 *
 * <ul>
 *   <li>`off` - always fused. The default until the tradeoff is measured on a cluster.
 *   <li>`auto` - defer when one unit's estimated payload volume reaches the threshold. With a
 *       single unit nothing is amplified and the deferral only adds a second read; a plan that
 *       small does not clear the default threshold.
 *   <li>`force` - defer whenever the result carries any payload column, for benchmarking the two
 *       shapes against each other on the same data.
 * </ul>
 */
object LancePayloadDeferral {
  val ModeConf = "spark.sql.lance.search.payload.deferral.mode"
  val MinBytesConf = "spark.sql.lance.search.payload.deferral.min.bytes"
  val ChunkRowsConf = "spark.sql.lance.search.payload.deferral.chunk.rows"

  val DefaultMode = "off"
  val DefaultMinBytes = 32L * 1024 * 1024
  val DefaultChunkRows = 1024

  /**
   * Upper bound on the payload bytes one fetch task asks lance-core for. `Dataset.takeRows` returns
   * the whole response as one Arrow IPC `byte[]`, so the chunk has to be sized in bytes, not only in
   * rows: a chunk is `min(chunk.rows, this / estimated row width)`.
   */
  val ChunkBytes = 16L * 1024 * 1024

  /** Nominal widths for variable-length types, used only to size chunks and the decision. */
  private val VariableWidthNominal = 128L

  /** Nominal width for a nested or collection column, likewise only a sizing estimate. */
  private val NestedNominal = 1024L

  final case class Settings(chunkRows: Int, estimatedRowWidth: Long, estimatedPayloadBytes: Long)

  /**
   * Returns the deferral settings when the payload should be fetched after ranking, or `None` for
   * the fused plan.
   */
  def plan(
      spark: SparkSession,
      schema: StructType,
      payloadColumns: Seq[String],
      candidates: Long): Option[Settings] = {
    if (payloadColumns.isEmpty || candidates <= 0) {
      return None
    }
    val mode = spark.conf.get(ModeConf, DefaultMode).trim.toLowerCase(Locale.ROOT)
    mode match {
      case "off" | "false" => None
      case "auto" | "true" =>
        val threshold = spark.conf.get(MinBytesConf, DefaultMinBytes.toString).trim.toLong
        val width = estimatedPayloadWidth(schema, payloadColumns)
        val bytes = estimatedBytes(candidates, width)
        if (bytes >= threshold) Some(settings(spark, width, bytes)) else None
      case "force" | "always" =>
        val width = estimatedPayloadWidth(schema, payloadColumns)
        Some(settings(spark, width, estimatedBytes(candidates, width)))
      case other =>
        throw new IllegalArgumentException(
          s"$ModeConf must be one of off, auto or force, but was '$other'")
    }
  }

  def estimatedBytes(candidates: Long, rowWidth: Long): Long = {
    if (candidates <= 0 || rowWidth <= 0) {
      0L
    } else if (candidates > Long.MaxValue / rowWidth) {
      Long.MaxValue
    } else {
      candidates * rowWidth
    }
  }

  /** Estimated in-memory width of one row of `payloadColumns`, from Spark types only. */
  def estimatedPayloadWidth(schema: StructType, payloadColumns: Seq[String]): Long = {
    payloadColumns.foldLeft(0L) { (total, name) =>
      val field = schema.fields.find(_.name == name)
      total + field.map(f => estimatedWidth(f.dataType)).getOrElse(VariableWidthNominal)
    }
  }

  def estimatedWidth(dataType: DataType): Long = dataType match {
    case BooleanType | ByteType => 1L
    case ShortType => 2L
    case IntegerType | FloatType | DateType => 4L
    case LongType | DoubleType | TimestampType => 8L
    case d: DecimalType => math.min(16L, math.max(1L, (d.precision + 1L) / 2L))
    case StringType | BinaryType => VariableWidthNominal
    case _: ArrayType | _: MapType | _: StructType => NestedNominal
    case _ => VariableWidthNominal
  }

  /** Rows per fetch task: the configured row count, capped so one response stays under the budget. */
  def chunkRows(configured: Int, rowWidth: Long): Int = {
    val byBytes =
      if (rowWidth <= 0) configured
      else math.max(1L, ChunkBytes / rowWidth)
    math.max(1, math.min(configured.toLong, byBytes).toInt)
  }

  private def settings(spark: SparkSession, width: Long, bytes: Long): Settings = {
    val configured = spark.conf.get(ChunkRowsConf, DefaultChunkRows.toString).trim.toInt
    val chunk = chunkRows(configured, width)
    Settings(chunk, width, bytes)
  }
}
