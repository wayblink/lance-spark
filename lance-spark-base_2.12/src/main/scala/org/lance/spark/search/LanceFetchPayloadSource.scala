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

import org.lance.spark.LanceSparkReadOptions
import org.lance.spark.internal.DatasetLeaseSettings

/**
 * Everything a deferred payload fetch needs to open the dataset again: the read options (pinned by
 * the scan), the credentials the catalog handed the driver, and the namespace identity used for the
 * executor-side credential refresh.
 *
 * <p>This exists as a holder, not as separate fields, so Spark's plan printing cannot leak it:
 * `TreeNode.stringArgs` prints a node's `productIterator`, and a plan is written to `EXPLAIN`, the
 * Spark UI SQL page and the event log's `physicalPlanDescription`. The `toString` below is the only
 * thing that can reach that output, and it carries no storage option values.
 */
class LanceFetchPayloadSource(
    val readOptions: LanceSparkReadOptions,
    val initialStorageOptions: java.util.Map[String, String],
    val namespaceImpl: String,
    val namespaceProperties: java.util.Map[String, String],
    val leaseSettings: DatasetLeaseSettings)
  extends Serializable {

  /**
   * The same holder with different read options, used to hand the fetch the version the scan
   * pinned. Everything else - the driver-side credentials, the namespace identity, the lease
   * settings - is unchanged because only the snapshot of the same dataset changes.
   */
  def withReadOptions(readOptions: LanceSparkReadOptions): LanceFetchPayloadSource =
    new LanceFetchPayloadSource(
      readOptions,
      initialStorageOptions,
      namespaceImpl,
      namespaceProperties,
      leaseSettings)

  /** Deliberately opaque: storage options are credentials and must not enter a plan string. */
  override def toString: String = "LanceFetchPayloadSource(credentials=redacted)"
}
