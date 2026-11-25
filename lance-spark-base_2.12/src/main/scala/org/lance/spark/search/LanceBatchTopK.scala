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
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

import java.util.Locale

/**
 * The global top-k of every query of a batch search, merged with k rows of state per query.
 *
 * <p>`child` is the distributed search scan: one task per searchable unit, each returning at most
 * `k` rows per query, so the candidates reaching this node are `units x queries x k` rows while the
 * answer is `queries x k`. Ranked per query, `k` rows are the only state this node needs: a row
 * outside a unit's local top k cannot enter the global top k, because the merge order is the same
 * order each unit used.
 *
 * <p>Rows are ordered by `_distance` ascending and, on a tie, by `_rowid` ascending - the order
 * lance-core ranks a single search in, so a batch answers what the same queries answer one at a
 * time. `_rowid` is always part of the ordering: a distance alone leaves tied rows on the k
 * boundary in task-arrival order, which makes the answer depend on scheduling.
 *
 * <p>Why an explicit node instead of `row_number() OVER (PARTITION BY query_index ...)`: Spark's
 * `InferWindowGroupLimit` rewrites the window into a bounded two-phase top-k only below
 * `spark.sql.optimizer.windowGroupLimitThreshold` (1000 in Spark 3.5), so a batch at `k = 2000`
 * falls back to a global `Sort` and a `WindowExec` that materialize all candidates. This node is
 * bounded at every k, and it also drops the map-side sort the window plan adds at small k: a unit
 * task already holds exactly `k` rows per query, so sorting them before the exchange truncates
 * nothing.
 *
 * <p>The node holds ranking state, not payload state: it is inserted below the deferred payload
 * fetch as well as in the fused plan, and in both cases it copies only the rows it keeps.
 */
case class LanceBatchTopK(
    child: LogicalPlan,
    k: Int,
    partitionBy: Seq[Attribute],
    orderBy: Seq[SortOrder])
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): LanceBatchTopK =
    copy(child = newChild)

  override def simpleString(maxFields: Int): String =
    s"LanceBatchTopK(k=$k, partition=${partitionBy.map(_.name).mkString("[", ", ", "]")}, " +
      s"order=${orderBy.map(_.sql).mkString("[", ", ", "]")})"
}

object LanceBatchTopK {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /**
   * How a batch search ranks its queries: `bounded` merges with `k` rows of state per query,
   * `spark` hands the ranking to Spark's window plan (`row_number()` then `rank <= k`).
   */
  val MergeModeConf = "spark.sql.lance.search.batch.merge.mode"

  val Bounded: String = "bounded"
  val Spark: String = "spark"

  val DefaultMergeMode: String = Bounded

  /** Whether the bounded merge is selected, and a clear error for a misspelled mode. */
  def isBounded(session: SparkSession): Boolean = {
    val configured =
      session.conf.getOption(MergeModeConf).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
    if (configured.isEmpty) {
      return DefaultMergeMode == Bounded
    }
    configured match {
      case Bounded => true
      case Spark =>
        logger.info(
          s"Batch VECTOR_SEARCH ranking uses Spark's window plan because $MergeModeConf=$Spark; " +
            "a batch above spark.sql.optimizer.windowGroupLimitThreshold materializes every " +
            "candidate")
        false
      case other =>
        throw new IllegalArgumentException(
          s"$MergeModeConf must be '$Bounded' or '$Spark', but was '$other'")
    }
  }
}
