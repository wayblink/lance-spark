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

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.connector.metric.CustomMetric;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sums the custom SQL metrics Spark attaches to each task-end event, by metric name.
 *
 * <p>Spark names the accumulator after the metric's description, so the reverse lookup goes through
 * the metric catalog. The accumulator's {@code update} is the task's own contribution; {@code
 * value} is the running total over all tasks so far and must not be summed.
 */
class LanceTaskMetricsListener extends SparkListener {
  private static final Map<String, String> DESC_TO_NAME = new HashMap<>();

  static {
    for (CustomMetric metric : LanceCustomMetrics.allMetrics()) {
      DESC_TO_NAME.put(metric.description(), metric.name());
    }
    for (CustomMetric metric : LanceCustomMetrics.distributedSearchMetrics()) {
      DESC_TO_NAME.put(metric.description(), metric.name());
    }
  }

  private final Map<String, Long> metricValues = new ConcurrentHashMap<>();

  Map<String, Long> metricValues() {
    return metricValues;
  }

  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    if (taskEnd.taskInfo() == null) {
      return;
    }
    JavaConverters.seqAsJavaList(taskEnd.taskInfo().accumulables())
        .forEach(
            info -> {
              if (info.name().isDefined() && info.update().isDefined()) {
                String name = DESC_TO_NAME.get(info.name().get());
                Object value = info.update().get();
                if (name != null && value instanceof Long) {
                  metricValues.merge(name, (Long) value, Long::sum);
                }
              }
            });
  }
}
