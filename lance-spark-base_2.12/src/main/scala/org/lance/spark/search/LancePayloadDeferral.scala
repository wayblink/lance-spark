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
 *   <li>`auto` - defer when one unit's estimated payload volume reaches the threshold. A payload
 *       column that cannot be fetched by row id makes it fall back to fused with a warning.
 *   <li>`force` - defer whenever the result carries a payload column, and fail if one of them
 *       cannot be fetched by row id instead of silently running the fused plan.
 * </ul>
 */
object LancePayloadDeferral {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  val ModeConf = "spark.sql.lance.search.payload.deferral.mode"
  val MinBytesConf = "spark.sql.lance.search.payload.deferral.min.bytes"
  val ChunkRowsConf = "spark.sql.lance.search.payload.deferral.chunk.rows"

  val DefaultMode = "off"
  val DefaultMinBytes = 32L * 1024 * 1024
  val DefaultChunkRows = 1024

  /** Nominal widths for variable-length types, used only to size the decision. */
  private val VariableWidthNominal = 128L

  /** Nominal width for a nested or collection column, likewise only a sizing estimate. */
  private val NestedNominal = 1024L

  final case class Settings(chunkRows: Int, estimatedRowWidth: Long, estimatedPayloadBytes: Long)

  /**
   * Returns the deferral settings when the payload should be fetched after ranking, or `None` for
   * the fused plan.
   *
   * @param unsupportedPayloadColumns payload columns that cannot be deferred (nested types, or a
   *     name that collides with an internal ranking column). `force` fails on them; `auto` falls
   *     back to fused and says why.
   */
  def plan(
      spark: SparkSession,
      schema: StructType,
      payloadColumns: Seq[String],
      candidates: Long,
      unsupportedPayloadColumns: Seq[String]): Option[Settings] = {
    if (payloadColumns.isEmpty || candidates <= 0) {
      return None
    }
    val mode = spark.conf.get(ModeConf, DefaultMode).trim.toLowerCase(Locale.ROOT)
    mode match {
      case "off" | "false" => None
      case "auto" | "true" =>
        if (unsupportedPayloadColumns.nonEmpty) {
          logger.warn(
            "VECTOR_SEARCH keeps the fused payload plan: {} cannot be fetched by row id",
            unsupportedPayloadColumns.mkString("[", ", ", "]"))
          return None
        }
        val threshold = spark.conf.get(MinBytesConf, DefaultMinBytes.toString).trim.toLong
        val width = estimatedPayloadWidth(schema, payloadColumns)
        val bytes = estimatedBytes(candidates, width)
        if (bytes >= threshold) Some(settings(spark, width, bytes)) else None
      case "force" | "always" =>
        if (unsupportedPayloadColumns.nonEmpty) {
          throw new IllegalArgumentException(
            s"$ModeConf=force cannot fetch payload column(s) "
              + unsupportedPayloadColumns.mkString("[", ", ", "]")
              + " by row id: a nested column does not round-trip through Dataset.takeRows with the "
              + "scan's Arrow layout, and a column named like an internal ranking column cannot be "
              + "told apart from it. Rename the column or set $ModeConf=off to use the fused plan.")
        }
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

  /**
   * Estimated width of one row of `payloadColumns`, from Spark types only.
   *
   * <p>Used for the planning decision and the log line, not as the memory bound: a Spark
   * `ArrayType` does not carry the fixed-size-list length, so a 2560 dimensional vector looks 10x
   * smaller here than it is. The fetch sizes each request from the dataset's real Arrow schema.
   */
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

  /** Configured row cap for one fetch request; the fetcher lowers it from the real Arrow width. */
  private def settings(spark: SparkSession, width: Long, bytes: Long): Settings = {
    val configured = spark.conf.get(ChunkRowsConf, DefaultChunkRows.toString).trim.toInt
    Settings(math.max(configured, 1), width, bytes)
  }
}
