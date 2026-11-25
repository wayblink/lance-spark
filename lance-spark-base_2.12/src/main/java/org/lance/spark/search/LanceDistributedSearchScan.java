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

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.index.Index;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.spark.read.metric.LanceCustomMetrics;
import org.lance.spark.utils.FieldPathUtils;
import org.lance.spark.utils.Utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Plans a vector search as one Spark task per searchable unit of the dataset: one task per segment
 * of the vector index, plus one flat-KNN task per fragment no segment covers (unless {@code
 * fast_search} asked for indexed data only).
 *
 * <p>Each unit returns its own local top {@code k}; merging them into the global top {@code k} is
 * the caller's job - {@code LanceSearchTableFunctions} wraps this scan in a global sort and limit.
 * The merge sorts by the distance each unit reported, so a unit never needs to return more than
 * {@code k} rows: a row outside its local top {@code k} cannot enter the global top {@code k}.
 */
public class LanceDistributedSearchScan implements Scan, Batch, Serializable {
  private static final long serialVersionUID = -917364523098172364L;
  private static final Logger LOG = LoggerFactory.getLogger(LanceDistributedSearchScan.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final String VECTOR_INDEX_DETAILS = "lance.index.pb.VectorIndexDetails";
  private static final String LEGACY_VECTOR_INDEX_DETAILS = "lance.index.VectorIndexDetails";

  /** Must stay the field namespace execution defaults to; see vector-search.md. */
  private static final String DEFAULT_VECTOR_COLUMN = "vector";

  private final StructType schema;
  private final LanceSearchQuery query;
  private final String scanId = UUID.randomUUID().toString();
  private transient long driverPlanningTimeNs;
  private transient long driverIndexDiscoveryTimeNs;

  /**
   * Planned once per scan. Spark builds a fresh {@code BatchScanExec} for every materialization of
   * the same logical plan, so replanning would pin a new dataset version each time: two actions on
   * one DataFrame could then read different versions. Driver-only, hence transient.
   */
  private transient InputPartition[] plannedPartitions;

  public LanceDistributedSearchScan(StructType schema, LanceSearchQuery query) {
    Objects.requireNonNull(
        query.getReadOptions(), "query.readOptions is required for distributed search");
    this.schema = schema;
    this.query = query;
  }

  @Override
  public StructType readSchema() {
    return schema;
  }

  @Override
  public String description() {
    return "LanceDistributedSearchScan [scanId=" + scanId + "]";
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new LanceDistributedSearchPartitionReaderFactory();
  }

  /**
   * The same per-task counters the read path reports, so a benchmark can attribute a wall-clock
   * difference to storage, to native search, or to neither (card #3089).
   */
  @Override
  public CustomMetric[] supportedCustomMetrics() {
    return LanceCustomMetrics.distributedSearchMetrics();
  }

  /** A snapshot only: reporting metrics must not trigger dataset or index I/O. */
  @Override
  public synchronized CustomTaskMetric[] reportDriverMetrics() {
    long planning = driverPlanningTimeNs;
    long discovery = driverIndexDiscoveryTimeNs;
    return new CustomTaskMetric[] {
      new CustomTaskMetric() {
        @Override
        public String name() {
          return LanceCustomMetrics.DRIVER_PLANNING_TIME_NS;
        }

        @Override
        public long value() {
          return planning;
        }
      },
      new CustomTaskMetric() {
        @Override
        public String name() {
          return LanceCustomMetrics.DRIVER_INDEX_DISCOVERY_TIME_NS;
        }

        @Override
        public long value() {
          return discovery;
        }
      }
    };
  }

  @Override
  public synchronized InputPartition[] planInputPartitions() {
    if (plannedPartitions == null) {
      plannedPartitions = planPartitionsOnce();
    }
    return plannedPartitions.clone();
  }

  private InputPartition[] planPartitionsOnce() {
    long started = System.nanoTime();
    driverIndexDiscoveryTimeNs = 0L;
    boolean succeeded = false;
    try {
      InputPartition[] partitions = planPartitions();
      succeeded = true;
      return partitions;
    } finally {
      driverPlanningTimeNs = System.nanoTime() - started;
      try {
        LOG.info(
            "Lance distributed search planning: scanId={}, succeeded={}, planningTimeNs={}, "
                + "indexDiscoveryTimeNs={}",
            scanId,
            succeeded,
            driverPlanningTimeNs,
            driverIndexDiscoveryTimeNs);
      } catch (RuntimeException ignored) {
        // Diagnostics must not replace a successful result or the original planning failure.
      }
    }
  }

  private InputPartition[] planPartitions() {
    Dataset dataset =
        Utils.openDatasetBuilder(query.getReadOptions())
            .initialStorageOptions(query.getInitialStorageOptions())
            .build();
    try {
      Set<Integer> existingFragments = new HashSet<>();
      for (Fragment fragment : dataset.getFragments()) {
        existingFragments.add(fragment.getId());
      }
      if (existingFragments.isEmpty()) {
        LOG.info("Lance distributed vector search: empty dataset, returning empty result");
        return new InputPartition[0];
      }

      boolean fastSearch = Boolean.TRUE.equals(query.getFastSearch());
      boolean bypassVectorIndex = Boolean.TRUE.equals(query.getBypassVectorIndex());
      String requestedIndex = query.getIndexName();
      if (requestedIndex != null && requestedIndex.isEmpty()) {
        // An empty name is a mistake, not "no name": treating it as absent would run some other
        // index while the caller believes they picked one.
        throw new IllegalArgumentException(
            "index must name a vector index; an empty name cannot select one");
      }
      long discoveryStart = System.nanoTime();
      NamedVectorIndex namedIndex;
      String column;
      Optional<VectorIndexInfo> vectorIndex;
      try {
        namedIndex =
          requestedIndex != null
              ? resolveNamedVectorIndex(dataset, requestedIndex, query.getVectorColumn())
              : null;
      // A named index decides the column, even when the search bypasses the index: the caller
      // named an index, so the column it was built on is the one they mean.
      column = namedIndex != null ? namedIndex.column : resolveVectorColumn();
      if (namedIndex != null && bypassVectorIndex) {
        // bypass_vector_index means "do not search an index"; naming one is not a conflict worth
        // failing on, but it must not look as if the named index was used.
        LOG.warn("Ignoring index {} because bypass_vector_index=true", requestedIndex);
      }
      vectorIndex =
          bypassVectorIndex
              ? Optional.empty()
              : selectVectorIndex(
                  dataset,
                  column,
                  query.getDistanceType(),
                  fastSearch,
                  namedIndex == null ? null : namedIndex.description,
                  namedIndex == null ? null : namedIndex.allIndices);
      } finally {
        driverIndexDiscoveryTimeNs = System.nanoTime() - discoveryStart;
      }

      // A segment whose fragments are all gone (compaction, deletion) is stale: no unit for it.
      Set<Integer> fallbackFragments = new TreeSet<>(existingFragments);
      List<UUID> liveSegments = new ArrayList<>();
      if (vectorIndex.isPresent()) {
        for (VectorIndexSegment segment : vectorIndex.get().getSegments()) {
          fallbackFragments.removeAll(segment.getFragmentIds());
          if (!Collections.disjoint(segment.getFragmentIds(), existingFragments)) {
            liveSegments.add(segment.getUuid());
          }
        }
      }
      if (fastSearch) {
        fallbackFragments.clear();
      }

      LanceSearchQuery resolvedQuery =
          query.toBuilder()
              .vectorColumn(column)
              .readOptions(
                  query
                      .getReadOptions()
                      .withRef(Utils.pinOpenedRef(dataset, query.getReadOptions().getRef())))
              .build();
      boolean mixedPlan = !liveSegments.isEmpty() && !fallbackFragments.isEmpty();
      // Indexed tasks search with the index's metric when none is given, while flat tasks would
      // default to L2; the global merge needs both on the same metric.
      if ((resolvedQuery.getDistanceType() == null || resolvedQuery.getDistanceType().isEmpty())
          && mixedPlan) {
        String indexMetric =
            vectorIndex
                .get()
                .getMetric()
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            "Cannot determine the distance metric for vector index '"
                                + vectorIndex.get().getIndexName()
                                + "'; pass distance_type explicitly before mixing indexed and "
                                + "unindexed fragments"));
        resolvedQuery = resolvedQuery.toBuilder().distanceType(indexMetric).build();
      }
      // A quantized index reports quantized distances while a flat unit reports exact ones, and
      // the merge ranks them against each other. refine_factor makes an indexed unit re-score
      // against the original vectors, which is what knn_combined does - and it too only does it
      // for a mixed plan, so an all-indexed plan is left reporting what namespace would report.
      // A factor of 1 re-scores without enlarging the candidate set: the gate is presence, not
      // value.
      LanceSearchQuery indexedQuery =
          mixedPlan && resolvedQuery.getRefineFactor() == null
              ? resolvedQuery.toBuilder().refineFactor(1).build()
              : resolvedQuery;

      List<LanceDistributedSearchInputPartition> units =
          planUnits(indexedQuery, resolvedQuery, liveSegments, fallbackFragments);

      long indexedCount = units.stream().filter(u -> !u.getIndexSegments().isEmpty()).count();
      LOG.info(
          "Lance distributed vector search: scanId={}, column={}, indexName={}, units={} "
              + "(indexed={}, fallback={}), candidateK={}",
          scanId,
          column,
          vectorIndex.map(VectorIndexInfo::getIndexName).orElse("none"),
          units.size(),
          indexedCount,
          units.size() - indexedCount,
          resolvedQuery.getK());

      return units.toArray(new InputPartition[0]);
    } finally {
      dataset.close();
    }
  }

  private List<LanceDistributedSearchInputPartition> planUnits(
      LanceSearchQuery indexedQuery,
      LanceSearchQuery fallbackQuery,
      List<UUID> liveSegments,
      Set<Integer> fallbackFragments) {
    List<LanceDistributedSearchInputPartition> units = new ArrayList<>();
    for (UUID segmentUuid : liveSegments) {
      units.add(
          LanceDistributedSearchInputPartition.forIndexSegment(schema, indexedQuery, segmentUuid));
    }
    for (Integer fragmentId : fallbackFragments) {
      units.add(
          LanceDistributedSearchInputPartition.forFragment(schema, fallbackQuery, fragmentId));
    }
    return units;
  }

  private String resolveVectorColumn() {
    String declared = query.getVectorColumn();
    if (declared != null && !declared.isEmpty()) {
      return declared;
    }
    return DEFAULT_VECTOR_COLUMN;
  }

  /** A named vector index, the column it indexes, and the listing it was found in. */
  private static final class NamedVectorIndex {
    private final IndexDescription description;
    private final String column;
    private final List<IndexDescription> allIndices;

    NamedVectorIndex(
        IndexDescription description, String column, List<IndexDescription> allIndices) {
      this.description = description;
      this.column = column;
      this.allIndices = allIndices;
    }
  }

  /**
   * The index the user named, or a failure that lists what can be named instead.
   *
   * <p>Deliberately never falls back: a name that does not resolve is a mistake in the query, and
   * searching some other index silently is how a benchmark measures the wrong thing.
   *
   * <p>The listing is returned to the caller so selection reuses it. Index discovery reads more
   * than names - on a large table the vector statistics it loads are the expensive part - so
   * listing twice per query would double a cost that has already been enough to OOM a driver.
   */
  private static NamedVectorIndex resolveNamedVectorIndex(
      Dataset dataset, String requestedIndex, String declaredColumn) {
    List<IndexDescription> indices;
    try {
      indices = dataset.describeIndices(new IndexCriteria.Builder().build());
    } catch (Exception e) {
      throw new IllegalStateException(
          "Cannot resolve vector index '" + requestedIndex + "': index discovery failed", e);
    }

    IndexDescription named = null;
    List<String> available = new ArrayList<>();
    for (IndexDescription idx : indices) {
      if (!isVectorIndex(idx)) {
        continue;
      }
      available.add(idx.getName());
      if (idx.getName().equals(requestedIndex)) {
        named = idx;
      }
    }
    if (named == null) {
      Collections.sort(available);
      throw new IllegalArgumentException(
          "Vector index '"
              + requestedIndex
              + "' does not exist"
              + (available.isEmpty()
                  ? "; the table has no vector indexes"
                  : ". Available vector indexes: " + String.join(", ", available)));
    }

    String indexColumn = columnOfIndex(dataset, named);
    if (declaredColumn != null
        && !declaredColumn.isEmpty()
        && !sameVectorColumn(declaredColumn, indexColumn)) {
      throw new IllegalArgumentException(
          "vector_column '"
              + declaredColumn
              + "' does not match vector index '"
              + requestedIndex
              + "', which is built on '"
              + indexColumn
              + "'");
    }
    return new NamedVectorIndex(named, indexColumn, indices);
  }

  /**
   * Whether the caller's {@code vector_column} names the same field as the canonical path an index
   * was built on.
   *
   * <p>Compared as parsed paths, not as strings: {@link FieldPathUtils#canonicalPath} quotes a part
   * only when it has to, so a column whose name contains a hyphen has the canonical form {@code
   * `my-vec`} while a caller may legitimately write {@code my-vec}.
   */
  static boolean sameVectorColumn(String declared, String canonical) {
    try {
      return FieldPathUtils.parseCanonicalPath(declared)
          .equals(FieldPathUtils.parseCanonicalPath(canonical));
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /** The column a vector index was built on, as the canonical path the scan needs. */
  private static String columnOfIndex(Dataset dataset, IndexDescription index) {
    List<Integer> fieldIds = index.getFieldIds();
    if (fieldIds == null || fieldIds.isEmpty()) {
      throw new IllegalArgumentException(
          "Vector index '" + index.getName() + "' does not record the column it indexes");
    }
    String column = FieldPathUtils.pathByFieldId(dataset.getLanceSchema(), fieldIds.get(0));
    if (column == null || column.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot resolve the column of vector index '"
              + index.getName()
              + "': field id "
              + fieldIds.get(0)
              + " is not in the dataset schema");
    }
    return column;
  }

  private static Optional<VectorIndexInfo> selectVectorIndex(
      Dataset dataset,
      String column,
      String requestedMetric,
      boolean fastSearch,
      IndexDescription namedIndex,
      List<IndexDescription> knownIndices) {
    List<IndexDescription> indices;
    if (knownIndices != null) {
      // The named path already listed the indexes; asking the dataset again would read the same
      // statistics twice.
      indices = knownIndices;
    } else {
      try {
        indices = dataset.describeIndices(new IndexCriteria.Builder().build());
      } catch (Exception e) {
        failFastSearchWhenIndexDiscoveryFails(fastSearch, e);
        LOG.warn("describeIndices failed, falling back to flat search: {}", e.getMessage());
        return Optional.empty();
      }
    }

    int columnFieldId;
    try {
      columnFieldId = FieldPathUtils.resolveField(dataset.getLanceSchema(), column).getId();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot find vector column '" + column + "' in the Lance schema", e);
    }

    // A column can carry several vector indexes, so the choice has to match lance-core's - unless
    // the user named one, in which case the name wins.
    Optional<IndexDescription> selected =
        namedIndex != null
            ? Optional.of(namedIndex)
            : firstIndexOnColumn(dataset, indices, columnFieldId);
    if (!selected.isPresent()) {
      return Optional.empty();
    }
    IndexDescription idx = selected.get();

    String normalizedRequested = normalizeMetric(requestedMetric);
    Optional<String> indexMetric = resolveIndexMetric(dataset, idx);
    if (!indexMetricIsUsable(normalizedRequested, indexMetric)) {
      if (namedIndex != null) {
        // Naming an index is a promise about which index the query uses; a metric mismatch is a
        // query error, not something to paper over by scanning flat.
        throw new IllegalArgumentException(
            "Cannot search vector index '"
                + idx.getName()
                + "' with distance_type '"
                + normalizedRequested
                + "': the index was built with '"
                + indexMetric.orElse("an unreadable metric")
                + "'. Omit distance_type to use the index metric, or query a matching index.");
      }
      if (indexMetric.isPresent()) {
        LOG.warn(
            "Ignoring vector index {} because query metric {} does not match index metric {}",
            idx.getName(),
            normalizedRequested,
            indexMetric.get());
      } else {
        LOG.warn(
            "Ignoring vector index {} because its metric could not be read from the index "
                + "metadata, so it cannot be confirmed to match query metric {}",
            idx.getName(),
            normalizedRequested);
      }
      if (fastSearch) {
        throw new IllegalArgumentException(
            "fast_search cannot use vector index '"
                + idx.getName()
                + "' on column '"
                + column
                + "' with distance_type '"
                + normalizedRequested
                + "'");
      }
      return Optional.empty();
    }

    List<VectorIndexSegment> segments = new ArrayList<>();
    for (Index segment : idx.getSegments()) {
      segments.add(
          new VectorIndexSegment(
              segment.uuid(), requireFragmentCoverage(idx.getName(), segment.fragments())));
    }
    return Optional.of(new VectorIndexInfo(idx.getName(), segments, indexMetric));
  }

  /**
   * The first index keyed on the column in manifest order, which is what lance-core searches.
   * {@code getIndexes()} is in that order; {@code describeIndices} is sorted by name, so it can
   * supply the segments and metric but cannot make the choice.
   */
  private static Optional<IndexDescription> firstIndexOnColumn(
      Dataset dataset, List<IndexDescription> indices, int columnFieldId) {
    Map<String, IndexDescription> vectorIndexesByName = new HashMap<>();
    for (IndexDescription idx : indices) {
      if (isVectorIndex(idx)) {
        vectorIndexesByName.put(idx.getName(), idx);
      }
    }
    if (vectorIndexesByName.isEmpty()) {
      return Optional.empty();
    }
    List<Index> manifestOrder;
    try {
      manifestOrder = dataset.getIndexes();
    } catch (Exception e) {
      LOG.warn("getIndexes failed, keeping the name-sorted order: {}", e.getMessage());
      manifestOrder = Collections.emptyList();
    }
    for (Index segment : manifestOrder) {
      if (segment.fields().isEmpty() || segment.fields().get(0) != columnFieldId) {
        continue;
      }
      IndexDescription idx = vectorIndexesByName.get(segment.name());
      if (idx != null) {
        return Optional.of(idx);
      }
    }
    // Nothing usable from getIndexes; fall back to the name-sorted order.
    for (IndexDescription idx : indices) {
      if (isVectorIndex(idx)
          && !idx.getFieldIds().isEmpty()
          && idx.getFieldIds().get(0) == columnFieldId) {
        return Optional.of(idx);
      }
    }
    return Optional.empty();
  }

  private static Optional<String> resolveIndexMetric(
      Dataset dataset, IndexDescription description) {
    String details = description.getDetailsJson();
    if (details != null && !details.trim().isEmpty()) {
      try {
        JsonNode metric = JSON_MAPPER.readTree(details).get("metric_type");
        if (metric != null && metric.isTextual()) {
          String normalized = normalizeMetric(metric.asText());
          if (normalized != null) {
            return Optional.of(normalized);
          }
        }
      } catch (Exception e) {
        LOG.warn(
            "Could not parse details for vector index {}: {}",
            description.getName(),
            e.getMessage());
      }
    }

    try {
      Set<String> metrics = indexMetricTypes(dataset.getIndexStatistics(description.getName()));
      if (metrics.size() == 1) {
        return Optional.of(metrics.iterator().next());
      }
    } catch (Exception e) {
      LOG.warn(
          "Could not read statistics for vector index {}: {}",
          description.getName(),
          e.getMessage());
    }
    return Optional.empty();
  }

  static Set<String> indexMetricTypes(Map<String, Object> statistics) {
    Set<String> metrics = new HashSet<>();
    Object indices = statistics.get("indices");
    if (!(indices instanceof Iterable)) {
      return metrics;
    }
    for (Object item : (Iterable<?>) indices) {
      if (!(item instanceof Map)) {
        continue;
      }
      Object metric = ((Map<?, ?>) item).get("metric_type");
      String normalized = metric == null ? null : normalizeMetric(String.valueOf(metric));
      if (normalized != null) {
        metrics.add(normalized);
      }
    }
    return metrics;
  }

  private static String normalizeMetric(String metric) {
    try {
      return LanceSearchQuery.canonicalizeDistanceType(metric);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Whether an index may answer a query asking for {@code requested}. An omitted metric accepts any
   * index. A requested one needs a resolved, equal index metric: an unverified segment handed to
   * {@code indexSegments(...)} makes lance-core fail the task rather than fall back, so losing the
   * index is the safe direction. A mismatch never moves on to the next index, matching lance-core,
   * where the metric only vetoes the index already chosen.
   */
  static boolean indexMetricIsUsable(String requested, Optional<String> indexMetric) {
    if (requested == null) {
      return true;
    }
    return indexMetric.isPresent() && requested.equals(indexMetric.get());
  }

  /**
   * Splitting a search into indexed and flat units needs to know which fragments each index segment
   * covers. Indices written before Lance 0.8 did not persist that bitmap, and lance-core refuses
   * the same plan for the same reason - {@code Dataset::unindexed_fragments} in
   * rust/lance/src/index.rs returns "Please upgrade lance to 0.8+ to use this function" - so
   * namespace execution fails on these datasets too.
   */
  static Set<Integer> requireFragmentCoverage(
      String indexName, Optional<List<Integer>> fragmentIds) {
    return fragmentIds
        .map(ids -> (Set<Integer>) new HashSet<>(ids))
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Vector index '"
                        + indexName
                        + "' has a segment without a fragment bitmap; please upgrade the dataset "
                        + "to Lance 0.8+ to search it"));
  }

  static void failFastSearchWhenIndexDiscoveryFails(boolean fastSearch, Exception cause) {
    if (fastSearch) {
      throw new IllegalStateException(
          "fast_search cannot continue because vector index discovery failed", cause);
    }
  }

  static boolean isVectorIndexTypeUrl(String typeUrl) {
    if (typeUrl == null) {
      return false;
    }
    int separator = typeUrl.lastIndexOf('/');
    if (separator < 0 || separator == typeUrl.length() - 1) {
      return false;
    }
    String detailsType = typeUrl.substring(separator + 1);
    return VECTOR_INDEX_DETAILS.equalsIgnoreCase(detailsType)
        || LEGACY_VECTOR_INDEX_DETAILS.equalsIgnoreCase(detailsType);
  }

  private static boolean isVectorIndex(IndexDescription idx) {
    return isVectorIndexTypeUrl(idx.getTypeUrl());
  }

  /** Lightweight view of a vector index for planning. */
  private static final class VectorIndexInfo {
    private final String indexName;
    private final List<VectorIndexSegment> segments;
    private final Optional<String> metric;

    VectorIndexInfo(String indexName, List<VectorIndexSegment> segments, Optional<String> metric) {
      this.indexName = indexName;
      this.segments = Collections.unmodifiableList(new ArrayList<>(segments));
      this.metric = metric;
    }

    String getIndexName() {
      return indexName;
    }

    List<VectorIndexSegment> getSegments() {
      return segments;
    }

    Optional<String> getMetric() {
      return metric;
    }
  }

  /** One physical segment of a vector index. */
  private static final class VectorIndexSegment {
    private final UUID uuid;
    private final Set<Integer> fragmentIds;

    VectorIndexSegment(UUID uuid, Set<Integer> fragmentIds) {
      this.uuid = uuid;
      this.fragmentIds = Collections.unmodifiableSet(new HashSet<>(fragmentIds));
    }

    UUID getUuid() {
      return uuid;
    }

    Set<Integer> getFragmentIds() {
      return fragmentIds;
    }
  }
}
