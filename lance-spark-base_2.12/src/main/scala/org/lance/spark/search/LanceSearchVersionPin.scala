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

/**
 * Carries the snapshot a distributed search planned on from the search scan to the payload fetch
 * that runs after it.
 *
 * <p>The scan pins the dataset version while it plans its tasks, which happens after this fetch
 * node was built. The fetch must read that same version: opening the dataset again would pin
 * "latest" a second time, and an append or delete in between would then either attach payload to
 * rows the search never saw or silently drop winners. Both objects live on the driver only, so this
 * holder is never serialized to an executor; tasks receive the pinned options through their
 * partitions.
 */
class LanceSearchVersionPin extends Serializable {
  @volatile private var pinned: LanceSparkReadOptions = _

  def record(options: LanceSparkReadOptions): Unit = pinned = options

  def lookup: LanceSparkReadOptions = pinned
}
