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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{LeafExpression, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

/**
 * Placeholder for a payload column a [[LanceFetchPayload]] node fills in from the row-id fetch.
 *
 * <p>Spark requires a plan to produce the attributes its parent references, so the child has to
 * carry the payload columns even though the value is fetched elsewhere. This expression supplies
 * them with the declared data type and nullability - a `Cast(null, ...)` would make a NOT NULL
 * column look nullable to the caller.
 *
 * <p>The value it evaluates to is a type-correct default, never null and never read: the fetch node
 * replaces it with the payload read by row id, and a null value for a column declared NOT NULL would
 * make Spark's generated projection fail. It is deliberately not foldable, so constant folding
 * cannot turn it into a plain literal and change the nullability the plan reports.
 */
case class LancePayloadPlaceholder(name: String, dataType: DataType, nullable: Boolean)
  extends LeafExpression
  with CodegenFallback {

  override def foldable: Boolean = false

  override def eval(input: InternalRow): Any = Literal.default(dataType).value

  override def toString: String = name
}
