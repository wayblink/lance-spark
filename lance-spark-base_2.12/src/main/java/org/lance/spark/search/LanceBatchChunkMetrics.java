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

import org.lance.spark.read.metric.LanceCustomMetrics;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * Per-task counters for a batch search that runs as several native scans. Allocated once per
 * reader, like {@code LanceReadMetricsTracker}, so the Spark metric sampling on every {@code
 * next()} allocates nothing.
 *
 * <p>All three counters are sums over tasks, so a Spark UI reader divides them by the number of
 * tasks to get the per-task value: `numBatchChunks / tasks` is the number of native scans one task
 * opened, `numBatchChunkQueries / tasks` is the batch size, and `numBatchChunkBytes / tasks` is the
 * estimated working set one task submitted. A single-query search leaves all three at zero.
 */
public final class LanceBatchChunkMetrics {
  private long chunks;
  private long queries;
  private long bytes;

  private final CustomTaskMetric[] taskMetrics =
      new CustomTaskMetric[] {
        new CustomTaskMetric() {
          @Override
          public String name() {
            return LanceCustomMetrics.NUM_BATCH_CHUNKS;
          }

          @Override
          public long value() {
            return chunks;
          }
        },
        new CustomTaskMetric() {
          @Override
          public String name() {
            return LanceCustomMetrics.NUM_BATCH_CHUNK_QUERIES;
          }

          @Override
          public long value() {
            return queries;
          }
        },
        new CustomTaskMetric() {
          @Override
          public String name() {
            return LanceCustomMetrics.NUM_BATCH_CHUNK_BYTES;
          }

          @Override
          public long value() {
            return bytes;
          }
        },
      };

  /**
   * Records one native scan of a batch search.
   *
   * @param queriesInChunk queries submitted to it
   * @param estimatedBytes its estimated query keys plus result rows
   */
  public void addChunk(int queriesInChunk, long estimatedBytes) {
    chunks++;
    queries += Math.max(queriesInChunk, 0);
    bytes += Math.max(estimatedBytes, 0L);
  }

  public CustomTaskMetric[] currentMetricsValues() {
    return taskMetrics;
  }
}
