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
package org.lance.spark.internal;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Objects;

/**
 * The dataset lease's settings, resolved on the driver and shipped to the tasks.
 *
 * <p>Executors cannot read {@code spark.conf}: a Spark session conf lives in the driver's {@code
 * SQLConf}, while an executor only has its {@code SparkConf}. Everything the executor side of a
 * search needs from configuration therefore has to travel with the plan, which is what this object
 * is for. It is also what makes the settings testable: a test changes the session conf and the next
 * query plans with the new values.
 */
public final class DatasetLeaseSettings implements Serializable {
  private static final long serialVersionUID = 4129384712938471293L;
  private static final Logger LOG = LoggerFactory.getLogger(DatasetLeaseSettings.class);

  /** Switch for the whole mechanism; {@code false} reproduces the pre-lease behaviour. */
  public static final String ENABLED_CONF = "spark.sql.lance.search.dataset.lease.enabled";

  /** How many idle datasets one executor keeps open. */
  public static final String MAX_ENTRIES_CONF = "spark.sql.lance.search.dataset.lease.max.entries";

  /** Age after which a lease is reopened instead of reused; bounds the namespace client's age. */
  public static final String MAX_AGE_MS_CONF = "spark.sql.lance.search.dataset.lease.max.age.ms";

  /** Idle time after which an unused lease is closed. */
  public static final String IDLE_TIMEOUT_MS_CONF =
      "spark.sql.lance.search.dataset.lease.idle.timeout.ms";

  public static final boolean DEFAULT_ENABLED = true;
  public static final int DEFAULT_MAX_ENTRIES = 8;
  public static final long DEFAULT_MAX_AGE_MS = 60_000L;
  public static final long DEFAULT_IDLE_TIMEOUT_MS = 60_000L;

  private static final DatasetLeaseSettings DEFAULTS =
      new DatasetLeaseSettings(
          DEFAULT_ENABLED, DEFAULT_MAX_ENTRIES, DEFAULT_MAX_AGE_MS, DEFAULT_IDLE_TIMEOUT_MS);

  private final boolean enabled;
  private final int maxEntries;
  private final long maxAgeMs;
  private final long idleTimeoutMs;

  public DatasetLeaseSettings(boolean enabled, int maxEntries, long maxAgeMs, long idleTimeoutMs) {
    this.enabled = enabled;
    this.maxEntries = Math.max(maxEntries, 0);
    this.maxAgeMs = Math.max(maxAgeMs, 0L);
    this.idleTimeoutMs = Math.max(idleTimeoutMs, 0L);
  }

  /** Reads the settings from the active session, or the defaults when there is none. */
  public static DatasetLeaseSettings fromActiveSession() {
    SparkSession spark;
    try {
      spark = SparkSession.active();
    } catch (RuntimeException e) {
      LOG.debug("No active Spark session; using the default search dataset lease settings");
      return DEFAULTS;
    }
    String enabled = spark.conf().get(ENABLED_CONF, String.valueOf(DEFAULT_ENABLED)).trim();
    if (!Boolean.parseBoolean(enabled)) {
      return new DatasetLeaseSettings(false, 0, DEFAULT_MAX_AGE_MS, DEFAULT_IDLE_TIMEOUT_MS);
    }
    return new DatasetLeaseSettings(
        true,
        readInt(spark, MAX_ENTRIES_CONF, DEFAULT_MAX_ENTRIES),
        readLong(spark, MAX_AGE_MS_CONF, DEFAULT_MAX_AGE_MS),
        readLong(spark, IDLE_TIMEOUT_MS_CONF, DEFAULT_IDLE_TIMEOUT_MS));
  }

  private static int readInt(SparkSession spark, String key, int defaultValue) {
    try {
      return Integer.parseInt(spark.conf().get(key, String.valueOf(defaultValue)).trim());
    } catch (NumberFormatException e) {
      LOG.warn("{} is not an integer; using {}", key, defaultValue);
      return defaultValue;
    }
  }

  private static long readLong(SparkSession spark, String key, long defaultValue) {
    try {
      return Long.parseLong(spark.conf().get(key, String.valueOf(defaultValue)).trim());
    } catch (NumberFormatException e) {
      LOG.warn("{} is not a long; using {}", key, defaultValue);
      return defaultValue;
    }
  }

  public static DatasetLeaseSettings defaults() {
    return DEFAULTS;
  }

  public boolean enabled() {
    return enabled;
  }

  public int maxEntries() {
    return maxEntries;
  }

  public long maxAgeMs() {
    return maxAgeMs;
  }

  public long idleTimeoutMs() {
    return idleTimeoutMs;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DatasetLeaseSettings)) {
      return false;
    }
    DatasetLeaseSettings that = (DatasetLeaseSettings) other;
    return enabled == that.enabled
        && maxEntries == that.maxEntries
        && maxAgeMs == that.maxAgeMs
        && idleTimeoutMs == that.idleTimeoutMs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled, maxEntries, maxAgeMs, idleTimeoutMs);
  }

  @Override
  public String toString() {
    return "DatasetLeaseSettings[enabled="
        + enabled
        + ", maxEntries="
        + maxEntries
        + ", maxAgeMs="
        + maxAgeMs
        + ", idleTimeoutMs="
        + idleTimeoutMs
        + "]";
  }
}
