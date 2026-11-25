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

import org.lance.Dataset;
import org.lance.spark.LanceRef;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Process-local reuse of opened Lance datasets for distributed search, so a task that searches the
 * same pinned version under the same credentials as an earlier task on this executor does not pay
 * the open again.
 *
 * <p>One open is not free: it reads the manifest and, through the namespace path, re-fetches the
 * table location and the vended storage options. A distributed search opens one dataset per task
 * (and, on the deferred payload path, one more per fetch task), so a query with hundreds of units
 * repeats that cost hundreds of times.
 *
 * <h2>The key</h2>
 *
 * <p>A lease is only reused when everything that decides what the dataset sees is identical:
 *
 * <ul>
 *   <li>the catalog (its Lance session, and therefore its caches),
 *   <li>the namespace identity (implementation plus a digest of its properties),
 *   <li>the resource ({@code tableId} for a namespace table, otherwise the dataset URI),
 *   <li>the pinned version — see below,
 *   <li>a digest of the merged storage options, i.e. the credentials, and
 *   <li>the open variant (block size and the per-query cache sizes).
 * </ul>
 *
 * <p>Both digests are stored, never the credential itself, because a key can reach a log line.
 *
 * <h2>What is not leased</h2>
 *
 * <p>A dataset is only leased when the read options pin a concrete version. "Latest" or a moving
 * tag would turn a cache hit into a query that silently reads an older snapshot than the caller
 * asked for, so those open per consumer exactly as before. In practice the distributed search
 * always pins: the scan resolves the version on the driver and hands every partition a {@code
 * main@version} reference.
 *
 * <h2>Credentials</h2>
 *
 * <p>Reuse never crosses a credential change: the storage-option digest is part of the key, so
 * rotated options are a miss and a fresh open. A lease also has a maximum age ({@code
 * spark.sql.lance.search.dataset.lease.max.age.ms}, one minute by default) after which it is
 * reopened rather than handed out, which rebuilds the executor-side namespace client. That keeps
 * the refresh semantics of {@link ExecutorNamespace}: a client is not kept alive indefinitely, and
 * a long task still refreshes vended credentials through the provider attached to its client.
 *
 * <h2>Isolation</h2>
 *
 * <p>The dataset is pinned to the version in the key, so sharing it across tasks cannot mix
 * snapshots: a task either gets a dataset on exactly the version it planned on, or opens its own.
 * Writers are unaffected: a cached dataset stays on its version and never sees a later commit.
 */
public final class DatasetLease {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetLease.class);

  private static final LeaseRegistry<OpenedDataset> REGISTRY =
      new LeaseRegistry<>(
          DatasetLeaseSettings.DEFAULT_MAX_ENTRIES,
          DatasetLeaseSettings.DEFAULT_MAX_AGE_MS,
          DatasetLeaseSettings.DEFAULT_IDLE_TIMEOUT_MS);

  private DatasetLease() {}

  /** A dataset plus the executor namespace client it was opened through. */
  static final class OpenedDataset implements AutoCloseable {
    private final Dataset dataset;
    private final ExecutorNamespace namespace;

    OpenedDataset(Dataset dataset, ExecutorNamespace namespace) {
      this.dataset = dataset;
      this.namespace = namespace;
    }

    @Override
    public void close() {
      try {
        dataset.close();
      } finally {
        namespace.close();
      }
    }
  }

  /** The lease a consumer holds; closing it releases this consumer's reference. */
  public static final class Handle implements AutoCloseable {
    private final Dataset dataset;
    private final LeaseRegistry.Handle<OpenedDataset> lease;
    private final boolean reused;
    private final boolean opened;

    private Handle(Dataset dataset, LeaseRegistry.Handle<OpenedDataset> lease) {
      this.dataset = dataset;
      this.lease = lease;
      this.reused = lease.reused();
      this.opened = lease.opened();
    }

    public Dataset dataset() {
      return dataset;
    }

    /** Whether this acquisition reused a dataset another consumer had already opened. */
    public boolean reused() {
      return reused;
    }

    /** Whether this acquisition opened the dataset. */
    public boolean opened() {
      return opened;
    }

    @Override
    public void close() {
      lease.close();
    }
  }

  /**
   * Borrows a dataset for the version, credentials and namespace in {@code readOptions}.
   *
   * <p>The caller must close the handle; the last consumer of a reusable lease returns it to the
   * cache (or closes it when caching is off).
   */
  public static Handle acquire(
      LanceSparkReadOptions readOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      DatasetLeaseSettings settings) {
    if (!settings.enabled()) {
      OpenedDataset opened =
          open(readOptions, initialStorageOptions, namespaceImpl, namespaceProperties);
      return new Handle(
          opened.dataset, new LeaseRegistry.Handle<>(opened, opened::close, false, true));
    }
    REGISTRY.configure(settings.maxEntries(), settings.maxAgeMs(), settings.idleTimeoutMs());

    LeaseRegistry.Key key =
        keyOf(readOptions, initialStorageOptions, namespaceImpl, namespaceProperties);
    if (key == null) {
      // Not a pinned version: reuse could cross a version boundary, so open per consumer.
      OpenedDataset opened =
          open(readOptions, initialStorageOptions, namespaceImpl, namespaceProperties);
      return new Handle(
          opened.dataset, new LeaseRegistry.Handle<>(opened, opened::close, false, true));
    }

    LeaseRegistry.Handle<OpenedDataset> lease =
        REGISTRY.acquire(
            key,
            () -> open(readOptions, initialStorageOptions, namespaceImpl, namespaceProperties));
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Lance dataset lease: {}, reused={}, opened={}", key, lease.reused(), lease.opened());
    }
    return new Handle(lease.value().dataset, lease);
  }

  /** Registry counters, for tests and for the per-task metrics the search reader reports. */
  public static LeaseRegistry.Stats stats() {
    return REGISTRY.stats();
  }

  /** Clears the process-wide cache. Only tests use this. */
  public static void resetForTest() {
    REGISTRY.clearForTest();
  }

  /**
   * The lease key, or null when the read options do not pin a version and therefore must not be
   * reused. Package-private so a unit test can assert exactly which differences split a lease
   * without opening a dataset.
   */
  static LeaseRegistry.Key keyOf(
      LanceSparkReadOptions readOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    String pinnedRef = pinnedRef(readOptions.getRef());
    if (pinnedRef == null) {
      return null;
    }
    String resource = resourceIdentity(readOptions);
    String identity = identityDigest(readOptions, namespaceImpl, namespaceProperties);
    String credentials = credentialDigest(readOptions, initialStorageOptions);
    String variant =
        "block="
            + readOptions.getBlockSize()
            + ";indexCache="
            + readOptions.getIndexCacheSize()
            + ";metadataCache="
            + readOptions.getMetadataCacheSize()
            + ";catalog="
            + readOptions.getCatalogName();
    return LeaseRegistry.Key.of(
        readOptions.getCatalogName(), identity, resource, pinnedRef, credentials, variant);
  }

  /**
   * Which namespace client and catalog a table id belongs to: a table id is only unique within one
   * namespace root, so a cache keyed on the id alone would answer one table with another's plan.
   */
  public static String identityDigest(
      LanceSparkReadOptions readOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    return digest(
        "catalog="
            + readOptions.getCatalogName()
            + ";impl="
            + namespaceImpl
            + ";props="
            + flatten(namespaceProperties)
            + ";refresh="
            + readOptions.isExecutorCredentialRefresh());
  }

  /** The resolved table: a namespace table id when there is one, otherwise the dataset URI. */
  public static String resourceIdentity(LanceSparkReadOptions readOptions) {
    return readOptions.getTableId() != null && !readOptions.getTableId().isEmpty()
        ? "table:" + String.join(".", readOptions.getTableId())
        : "uri:" + readOptions.getDatasetUri();
  }

  /** A digest of the merged storage options, i.e. the credentials, never the credentials. */
  private static String credentialDigest(
      LanceSparkReadOptions readOptions, Map<String, String> initialStorageOptions) {
    Map<String, String> merged = new TreeMap<>();
    if (readOptions.getStorageOptions() != null) {
      merged.putAll(readOptions.getStorageOptions());
    }
    if (initialStorageOptions != null) {
      merged.putAll(initialStorageOptions);
    }
    return digest(flatten(merged));
  }

  /** A concrete snapshot reference, or null when the reference can still move. */
  private static String pinnedRef(LanceRef ref) {
    if (ref == null || !ref.getVersionNumber().isPresent()) {
      return null;
    }
    long version = ref.getVersionNumber().get();
    if (ref.isTag()) {
      // A tag name can be moved to another version; the version alone is what the open resolves,
      // and the builder resolves a tag through a checkout, not through this number.
      return null;
    }
    if (ref.isBranch()) {
      return "branch:" + ref.getBranchName().get() + "@" + version;
    }
    return "main@" + version;
  }

  private static OpenedDataset open(
      LanceSparkReadOptions readOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties) {
    ExecutorNamespace namespace =
        ExecutorNamespace.acquire(readOptions, namespaceImpl, namespaceProperties);
    try {
      Dataset dataset =
          Utils.openDatasetBuilder(readOptions)
              .initialStorageOptions(initialStorageOptions)
              .build();
      return new OpenedDataset(dataset, namespace);
    } catch (RuntimeException | Error e) {
      namespace.close();
      throw e;
    }
  }

  private static String flatten(Map<String, String> values) {
    if (values == null || values.isEmpty()) {
      return "";
    }
    // Sorted so that two equal maps always digest to the same value.
    Map<String, String> sorted = new TreeMap<>(values);
    List<String> pairs = new ArrayList<>(sorted.size());
    for (Map.Entry<String, String> entry : sorted.entrySet()) {
      pairs.add(entry.getKey() + "=" + entry.getValue());
    }
    return String.join("\u0000", pairs);
  }

  private static String digest(String value) {
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      byte[] hashed = sha.digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(hashed.length * 2);
      for (byte b : hashed) {
        hex.append(Character.forDigit((b >> 4) & 0xF, 16));
        hex.append(Character.forDigit(b & 0xF, 16));
      }
      return hex.substring(0, 16);
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is mandatory on every JVM; falling back to a hash code would silently weaken the
      // credential boundary, so fail instead.
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }
}
