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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

/**
 * Materializes the payload columns of an already ranked search result by row id.
 *
 * <p>`child` is the narrow search plan: it selects the global winners in rank order but carries
 * only the columns ranking needs (`_distance`, `_rowid`, and `query_index` for a batch). This node
 * adds the remaining columns of `columns` by fetching them from the same pinned snapshot, so the
 * per-unit copies of the payload never reach Spark's sort or exchange.
 *
 * <p>Column identity is positional, not name-based: `narrowColumns` names the internal columns the
 * child carries in order, so a real table column that happens to be called `query_index` cannot be
 * mistaken for the batch's internal one.
 *
 * <p>Choosing this shape is a cost decision made by [[LancePayloadDeferral]]; the fused plan reads
 * the payload inside the native search instead.
 */
case class LanceFetchPayload(
    child: LogicalPlan,
    columns: Seq[Attribute],
    fetched: Seq[Attribute],
    narrowColumns: Seq[String],
    isBatch: Boolean,
    source: LanceFetchPayloadSource,
    chunkRows: Int,
    pin: LanceSearchVersionPin)
  extends UnaryNode {

  override def output: Seq[Attribute] = columns

  override protected def withNewChildInternal(newChild: LogicalPlan): LanceFetchPayload =
    copy(child = newChild)

  /** Plan text names the columns and the chunk size only: no credentials, no object identity. */
  override def simpleString(maxFields: Int): String =
    s"LanceFetchPayload(${fetched.map(_.name).mkString("[", ", ", "]")}, " +
      s"narrow=${narrowColumns.mkString("[", ", ", "]")}, batch=$isBatch, chunkRows=$chunkRows)"
}
