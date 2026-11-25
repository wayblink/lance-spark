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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.lance.spark.search.{LanceFetchPayloadSource, LancePayloadFetcher, LancePayloadMetrics, LanceSearchProjection, LanceSearchVersionPin}

import java.util.NoSuchElementException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Second phase of a deferred distributed vector search: the child produced the global winners (`k`
 * per query, in rank order) with only the ranking columns, and this node materializes the payload
 * for exactly those rows.
 *
 * <p>The winners are collected on the driver so the payload fetch can be split into bounded chunks
 * that each read by row id at the pinned snapshot; the collected data is `k x queries` rows of at
 * most three narrow columns. This is the same driver-side materialization Spark itself uses for a
 * scalar subquery, and it runs on the action thread while the plan executes - not during planning,
 * where a nested job could block Spark's DAGScheduler event thread.
 *
 * <p>A failed fetch task is retried by Spark and re-reads its chunk from the pinned snapshot, so a
 * retry produces the same rows: no state is carried across attempts.
 *
 * <p>The node does not publish an `outputOrdering`: the winners are ranked, but the fetch splits
 * them over several partitions, so claiming an order would be a per-partition claim an outer
 * `ORDER BY` cannot rely on. An outer sort therefore still sorts, which is what the fused plan
 * does as well.
 */
case class LanceFetchPayloadExec(
    child: SparkPlan,
    columns: Seq[Attribute],
    fetched: Seq[Attribute],
    narrowColumns: Seq[String],
    isBatch: Boolean,
    source: LanceFetchPayloadSource,
    chunkRows: Int,
    pin: LanceSearchVersionPin)
  extends UnaryExecNode {

  override def output: Seq[Attribute] = columns

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "LanceFetchPayload"

  /**
   * Plan text names the columns and the chunk size only. `productIterator` would otherwise print
   * the credential holder, and a plan string reaches `EXPLAIN`, the Spark UI SQL page and the
   * event log's `physicalPlanDescription`.
   */
  override def stringArgs: Iterator[Any] =
    Iterator(
      fetched.map(_.name).mkString("[", ", ", "]"),
      narrowColumns.mkString("[", ", ", "]"),
      isBatch,
      chunkRows)

  override lazy val metrics: Map[String, SQLMetric] =
    Map(
      "numDeferredPayloadRequests" -> SQLMetrics.createMetric(
        sparkContext,
        "number of takeRows requests for the deferred payload"),
      "numDeferredPayloadRows" -> SQLMetrics.createMetric(
        sparkContext,
        "number of rows read for the deferred payload"),
      "numDeferredPayloadBytes" -> SQLMetrics.createMetric(
        sparkContext,
        "decoded payload bytes read for the deferred payload"))

  private def payloadMetrics: LancePayloadMetrics = new LancePayloadMetrics {
    override def add(requests: Int, rows: Long, decodedBytes: Long): Unit = {
      if (requests > 0) {
        metrics("numDeferredPayloadRequests").add(requests)
      }
      if (rows > 0) {
        metrics("numDeferredPayloadRows").add(rows)
      }
      if (decodedBytes > 0) {
        metrics("numDeferredPayloadBytes").add(decodedBytes)
      }
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // The narrow columns are the child's output minus the payload placeholders: a set difference,
    // not a name lookup, so a real table column called `query_index` is payload and cannot be
    // mistaken for the batch's internal one. The order has to be the planner's ranking layout.
    val fetchedNames = fetched.map(_.name).toSet
    val narrowIndexes =
      child.output.indices.filterNot(position => fetchedNames.contains(child.output(position).name))
    val narrow = narrowIndexes.map(child.output(_).name)
    if (narrow != narrowColumns) {
      throw new IllegalStateException(
        "A deferred vector search expects the narrow columns " + narrowColumns.mkString(
          "[",
          ", ",
          "]") +
          " below the payload fetch, but got " + narrow.mkString("[", ", ", "]"))
    }
    val distanceIndex = narrow.indexWhere(_ == LanceSearchProjection.DISTANCE_COLUMN)
    val rowIdIndex = narrow.indexWhere(_ == LancePayloadFetcher.ROW_ID_COLUMN)
    val queryIndexIndex = narrow.indexWhere(_ == LanceSearchProjection.QUERY_INDEX)
    if (distanceIndex < 0 || rowIdIndex < 0) {
      throw new IllegalStateException(
        "A deferred vector search needs _distance and _rowid below the payload fetch, but got " +
          narrow.mkString("[", ", ", "]"))
    }
    val distancePosition = narrowIndexes(distanceIndex)
    val rowIdPosition = narrowIndexes(rowIdIndex)
    val queryIndexPosition = if (queryIndexIndex >= 0) narrowIndexes(queryIndexIndex) else -1

    // The child already applied the global ranking, so its order and k rows per query are the
    // answer; only the row identities have to survive into the fetch. Extracting primitives keeps
    // this bounded by three columns per winner instead of copying whole rows.
    val winners: Array[(Long, Float, Int)] =
      child
        .execute()
        .mapPartitions { rows =>
          val collected = new ArrayBuffer[(Long, Float, Int)]()
          while (rows.hasNext) {
            val row = rows.next()
            val queryIndex = if (queryIndexPosition >= 0) row.getInt(queryIndexPosition) else -1
            collected += ((row.getLong(rowIdPosition), row.getFloat(distancePosition), queryIndex))
          }
          collected.iterator
        }
        .collect()

    val sc = sparkContext
    if (winners.isEmpty) {
      return sc.emptyRDD[ColumnarBatch]
    }

    // The scan recorded the snapshot while it planned its tasks, which happens after this plan was
    // built and before the collect above returned. A missing pin means the fetch would open
    // "latest" - exactly the race the deferred path must not have - so it fails instead.
    val pinned = Option(pin.lookup).getOrElse {
      throw new IllegalStateException(
        "The deferred VECTOR_SEARCH payload fetch found no pinned snapshot: the search did not " +
          "record the version it ran on. Refusing to read the payload from the latest version, " +
          "which could mix two snapshots. Disable " +
          "spark.sql.lance.search.payload.deferral.mode to use the fused plan.")
    }
    // The source was built while planning, when no version was known yet, so opening it as-is
    // would read latest. Handing the fetcher a copy with the pinned ref is what puts the payload
    // read on the same snapshot the ranking saw; the fetcher itself refuses an unpinned source.
    val pinnedSource = source.withReadOptions(pinned)
    val schema = StructType(columns.map(a => StructField(a.name, a.dataType, a.nullable)))
    val fetcher =
      new LancePayloadFetcher(
        pinnedSource,
        schema,
        fetched.map(_.name).asJava,
        narrow.asJava,
        chunkRows,
        payloadMetrics)

    val chunks = winners.grouped(math.max(chunkRows, 1)).toSeq
    sc.parallelize(chunks, chunks.length).mapPartitions { chunkIterator =>
      chunkIterator.flatMap { chunk =>
        val rowIds = new Array[Long](chunk.length)
        val distances = new Array[Float](chunk.length)
        val queryIndexes = new Array[Int](chunk.length)
        var i = 0
        while (i < chunk.length) {
          val winner = chunk(i)
          rowIds(i) = winner._1
          distances(i) = winner._2
          queryIndexes(i) = winner._3
          i += 1
        }
        fetcher.fetch(rowIds, distances, queryIndexes).asScala
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    doExecuteColumnar().mapPartitions { batches =>
      new Iterator[InternalRow] {
        private var current: Iterator[InternalRow] = Iterator.empty
        private var batch: ColumnarBatch = _

        private def releaseBatch(): Unit = {
          if (batch != null) {
            batch.close()
            batch = null
          }
        }

        override def hasNext: Boolean = {
          while (!current.hasNext) {
            releaseBatch()
            if (!batches.hasNext) {
              return false
            }
            batch = batches.next()
            current = batch.rowIterator().asScala
          }
          true
        }

        override def next(): InternalRow = {
          if (!hasNext) {
            throw new NoSuchElementException("no more deferred search rows")
          }
          current.next()
        }
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
