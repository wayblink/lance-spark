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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Descending, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{DataType, FloatType, LongType}

import java.util.{Comparator, PriorityQueue}

import scala.collection.mutable.ArrayBuffer

/**
 * The bounded per-query top-k merge of a batch search: every row of one query is hash-partitioned
 * to the same task, so one task can decide that query's global top k with `k` rows of state.
 *
 * <p>State is the only thing this node bounds, and that state is the answer: across all tasks the
 * retained rows are exactly the `k` rows of each query (fewer when the table holds fewer matches),
 * so the merge cannot hold the `units x queries x k` candidates the per-unit scans produce. A row
 * enters a bounded heap per query and is copied only when it is among the best `k` seen so far.
 *
 * <p>There is no sort here, and none below: a top-k heap does not need its input ordered and the
 * scan already returns each unit's rows ranked. That is what separates this node from the window
 * plan it replaces, where `Sort` and `Window` materialize every candidate - and, above
 * `spark.sql.optimizer.windowGroupLimitThreshold`, do it in one global pass.
 *
 * <p>The node is row-based on purpose: retaining a row has to copy it, and the columnar batches a
 * scan produces are per-unit and transient. The conversion is the one the window plan already paid,
 * because `WindowExec` and `SortExec` are row-based too.
 */
case class LanceBatchTopKExec(
    child: SparkPlan,
    k: Int,
    partitionBy: Seq[Attribute],
    orderBy: Seq[SortOrder])
  extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = false

  override def nodeName: String = "LanceBatchTopK"

  override def stringArgs: Iterator[Any] =
    Iterator(k, partitionBy.map(_.name).mkString("[", ", ", "]"), orderBy.map(_.sql))

  override lazy val metrics: Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(
        sparkContext,
        "candidate rows fed to the per-query top-k merge"),
      "numRetainedRows" -> SQLMetrics.createMetric(
        sparkContext,
        "rows kept by the merge: the number of rows this node returns"),
      "numDistinctQueries" -> SQLMetrics.createMetric(
        sparkContext,
        "query groups this node ranked"))

  /**
   * One query's candidates must meet in one task. A clustered distribution asks Spark for exactly
   * that and nothing more: no ordered distribution, so no sort is inserted below the merge.
   */
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(distributionAttributes) :: Nil

  /** The position of an attribute in the child output, resolved by identity, not by name. */
  private def positionOf(attribute: Attribute, role: String): Int = {
    val byIdentity = child.output.indexWhere(_.exprId == attribute.exprId)
    if (byIdentity >= 0) {
      return byIdentity
    }
    // A plan rewrite may have replaced the attribute but kept its name; a name that matches more
    // than one column is ambiguous (a table column can be called `query_index` too), so only an
    // unambiguous name is accepted.
    val byName = child.output.zipWithIndex.filter(_._1.name == attribute.name)
    byName match {
      case Seq((_, position)) => position
      case Seq() =>
        throw new IllegalStateException(
          s"LanceBatchTopK needs the $role column '${attribute.name}', but the child output is " +
            child.output.map(_.name).mkString("[", ", ", "]"))
      case _ =>
        throw new IllegalStateException(
          s"LanceBatchTopK cannot tell which '${attribute.name}' column is the $role one: the " +
            s"child output carries ${byName.size} of them")
    }
  }

  private def rankingAttribute(order: SortOrder, role: String): Attribute = order.child match {
    case attribute: Attribute => attribute
    case other =>
      throw new IllegalStateException(
        s"LanceBatchTopK ranks by columns, but its $role order is an expression: ${other.sql}")
  }

  @transient private lazy val distributionAttributes: Seq[Attribute] =
    partitionBy.map(attribute => child.output(positionOf(attribute, "query attribution")))

  @transient private lazy val plan: MergePlan = {
    val queryPosition = positionOf(partitionBy.head, "query attribution")
    val attributes = orderBy.map(order => rankingAttribute(order, "ranking"))
    MergePlan(
      queryPosition,
      attributes.map(attribute => positionOf(attribute, "ranking")).toArray,
      attributes.map(_.dataType).toArray,
      orderBy.map(_.direction == Descending).toArray,
      math.max(k, 0))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRows = metrics("numInputRows")
    val retainedRows = metrics("numRetainedRows")
    val distinctQueries = metrics("numDistinctQueries")
    val merge = plan
    child.execute().mapPartitions { rows =>
      merged(rows, merge, inputRows, retainedRows, distinctQueries)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  /** Where each ranking key lives in the child rows, resolved once on the driver. */
  private case class MergePlan(
      queryPosition: Int,
      orderPositions: Array[Int],
      orderTypes: Array[DataType],
      descending: Array[Boolean],
      bound: Int)

  /**
   * Keeps the best `bound` rows of every query of one partition, then returns them grouped by query
   * in ascending query order with each group ranked. The heap head is the worst row kept, so a
   * candidate replaces it only when the candidate ranks better.
   */
  private def merged(
      rows: Iterator[InternalRow],
      merge: MergePlan,
      inputRows: SQLMetric,
      retainedRows: SQLMetric,
      distinctQueries: SQLMetric): Iterator[InternalRow] = {
    val bound = merge.bound
    if (bound <= 0) {
      return Iterator.empty
    }
    val bestFirst = new Comparator[InternalRow] {
      override def compare(left: InternalRow, right: InternalRow): Int =
        compareRows(left, right, merge)
    }
    val worstFirst = new Comparator[InternalRow] {
      override def compare(left: InternalRow, right: InternalRow): Int =
        compareRows(right, left, merge)
    }
    val heaps = new java.util.HashMap[Int, PriorityQueue[InternalRow]]()
    while (rows.hasNext) {
      val row = rows.next()
      inputRows.add(1)
      val query = row.getInt(merge.queryPosition)
      var heap = heaps.get(query)
      if (heap == null) {
        heap = new PriorityQueue[InternalRow](math.min(bound, 1024), worstFirst)
        heaps.put(query, heap)
      }
      if (heap.size < bound) {
        heap.add(row.copy())
      } else if (bestFirst.compare(row, heap.peek()) < 0) {
        // The candidate ranks better than the worst row kept, so it takes that row's place.
        heap.poll()
        heap.add(row.copy())
      }
    }

    var retained = 0L
    val heapIterator = heaps.values().iterator()
    while (heapIterator.hasNext) {
      retained += heapIterator.next().size
    }
    retainedRows.add(retained)
    distinctQueries.add(heaps.size())

    // Queries in ascending id order, which is the order a batch numbers its results in. Each
    // group is drained from its worst-first heap, sorted best-first, and appended.
    val queries = new Array[Int](heaps.size())
    val keyIterator = heaps.keySet().iterator()
    var keyIndex = 0
    while (keyIterator.hasNext) {
      queries(keyIndex) = keyIterator.next()
      keyIndex += 1
    }
    java.util.Arrays.sort(queries)
    val result = new ArrayBuffer[InternalRow](retained.toInt)
    var queryIndex = 0
    while (queryIndex < queries.length) {
      val heap = heaps.get(queries(queryIndex))
      val ranked = new Array[InternalRow](heap.size)
      var index = ranked.length - 1
      while (index >= 0) {
        // Draining a worst-first heap yields the best row last, so fill the output from the end.
        ranked(index) = heap.poll()
        index -= 1
      }
      java.util.Arrays.sort(
        ranked.asInstanceOf[Array[AnyRef]],
        bestFirst.asInstanceOf[Comparator[AnyRef]])
      var position = 0
      while (position < ranked.length) {
        result += ranked(position)
        position += 1
      }
      queryIndex += 1
    }
    result.iterator
  }

  /**
   * Compares a candidate pair by the ranking keys only: `_distance` ascending, then `_rowid`
   * ascending, which is the order lance-core ranks a single search in and the order the window plan
   * used. Nulls sort first, matching `SortOrder(_, Ascending)`.
   */
  private def compareRows(left: InternalRow, right: InternalRow, merge: MergePlan): Int = {
    var index = 0
    while (index < merge.orderPositions.length) {
      val position = merge.orderPositions(index)
      val order = compareValue(left, right, position, merge.orderTypes(index))
      if (order != 0) {
        return if (merge.descending(index)) -order else order
      }
      index += 1
    }
    0
  }

  private def compareValue(
      left: InternalRow,
      right: InternalRow,
      position: Int,
      dataType: DataType): Int = {
    val leftNull = left.isNullAt(position)
    val rightNull = right.isNullAt(position)
    if (leftNull) {
      return if (rightNull) 0 else -1
    }
    if (rightNull) {
      return 1
    }
    if (dataType == FloatType) {
      java.lang.Float.compare(left.getFloat(position), right.getFloat(position))
    } else if (dataType == LongType) {
      java.lang.Long.compare(left.getLong(position), right.getLong(position))
    } else {
      throw new IllegalStateException(
        s"LanceBatchTopK ranks by a float distance and a long row id, not by $dataType")
    }
  }
}
