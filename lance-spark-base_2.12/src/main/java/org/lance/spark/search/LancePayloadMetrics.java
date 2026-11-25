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
package org.lance.spark.search;

import java.io.Serializable;

/**
 * Where a deferred payload fetch reports its work: one {@code takeRows} request, one Arrow batch of
 * rows, and the decoded payload bytes those rows occupy.
 *
 * <p>An interface rather than Spark's {@code SQLMetric} so the fetcher can be tested and reused
 * without a {@code SparkContext}; the plan node supplies an implementation that adds to SQL
 * metrics.
 */
public interface LancePayloadMetrics extends Serializable {
  void add(int requests, long rows, long decodedBytes);
}
