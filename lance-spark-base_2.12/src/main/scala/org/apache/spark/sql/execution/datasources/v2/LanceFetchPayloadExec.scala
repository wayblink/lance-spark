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
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.lance.spark.LanceSparkReadOptions
import org.lance.spark.search.{LancePayloadFetcher, LanceSearchProjection, LanceSearchVersionPin}

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
 */
case class LanceFetchPayloadExec(
    child: SparkPlan,
    columns: Seq[Attribute],
    fetched: Seq[Attribute],
    readOptions: LanceSparkReadOptions,
    initialStorageOptions: java.util.Map[String, String],
    chunkRows: Int,
    pin: LanceSearchVersionPin)
  extends UnaryExecNode {

  override def output: Seq[Attribute] = columns

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "LanceFetchPayload"

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val distanceIndex = child.output.indexWhere(_.name == LanceSearchProjection.DISTANCE_COLUMN)
    val rowIdIndex = child.output.indexWhere(_.name == LancePayloadFetcher.ROW_ID_COLUMN)
    if (distanceIndex < 0 || rowIdIndex < 0) {
      throw new IllegalStateException(
        "A deferred vector search needs _distance and _rowid below the payload fetch, but got " +
          child.output.map(_.name).mkString("[", ", ", "]"))
    }
    val queryIndexIndex = child.output.indexWhere(_.name == LanceSearchProjection.QUERY_INDEX)

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
            val queryIndex = if (queryIndexIndex >= 0) row.getInt(queryIndexIndex) else -1
            collected += ((row.getLong(rowIdIndex), row.getFloat(distanceIndex), queryIndex))
          }
          collected.iterator
        }
        .collect()

    val sc = sparkContext
    if (winners.isEmpty) {
      return sc.emptyRDD[ColumnarBatch]
    }

    // The scan pinned the snapshot while it planned its tasks, which happens after this plan was
    // built. Reading the pin here is what keeps the fetch on the searched version instead of
    // opening "latest" a second time.
    val pinned = Option(pin.lookup).getOrElse(readOptions)
    val schema =
      StructType(columns.map(a => StructField(a.name, a.dataType, a.nullable)))
    val fetcher =
      new LancePayloadFetcher(
        pinned,
        initialStorageOptions,
        schema,
        fetched.map(_.name).asJava)

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
