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

import org.lance.namespace.model.QueryTableRequest;
import org.lance.namespace.model.QueryTableRequestColumns;
import org.lance.namespace.model.QueryTableRequestFullTextQuery;
import org.lance.namespace.model.QueryTableRequestVector;
import org.lance.namespace.model.StringFtsQuery;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.FullTextQueryConverter;
import org.lance.spark.utils.FullTextQueryUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class LanceSearchQuery implements Serializable {
  private static final long serialVersionUID = 538912738912739128L;

  public enum SearchType {
    VECTOR,
    FULL_TEXT
  }

  private final SearchType searchType;
  private final List<String> tableId;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final LanceSparkReadOptions readOptions;
  private final Map<String, String> initialStorageOptions;
  private final List<String> outputColumns;
  private final Integer k;
  private final Integer offset;
  private final Long version;
  private final String filter;
  private final Boolean withRowId;
  private final List<Float> vector;

  /**
   * Batch of query vectors, {@code queries * dimension} floats in row-major order. Empty for a
   * single-vector search, whose {@link #vector} field then carries that vector unchanged.
   */
  private final List<Float> vectors;

  /** Number of query vectors in a batch; 0 for a single-vector search. */
  private final int queryCount;

  /** Dimension of each query vector in a batch; 0 for a single-vector search. */
  private final int vectorDim;

  private final String vectorColumn;
  private final String indexName;
  private final String distanceType;
  private final Integer nprobes;
  private final Integer ef;
  private final Integer refineFactor;
  private final Float lowerBound;
  private final Float upperBound;
  private final Boolean bypassVectorIndex;
  private final Boolean fastSearch;
  private final Boolean prefilter;
  private final String textQuery;
  private final List<String> searchColumns;
  private final String fullTextQueryJson;

  private LanceSearchQuery(Builder builder) {
    this.searchType = builder.searchType;
    this.tableId = immutableList(builder.tableId);
    this.namespaceImpl = builder.namespaceImpl;
    this.namespaceProperties = immutableMap(builder.namespaceProperties);
    this.readOptions = builder.readOptions;
    this.initialStorageOptions = immutableMap(builder.initialStorageOptions);
    this.outputColumns = immutableList(builder.outputColumns);
    this.k = builder.k;
    this.offset = builder.offset;
    this.version = builder.version;
    this.filter = builder.filter;
    this.withRowId = builder.withRowId;
    this.vector = immutableList(builder.vector);
    this.vectors = immutableList(builder.vectors);
    this.queryCount = builder.queryCount;
    this.vectorDim = builder.vectorDim;
    this.vectorColumn = builder.vectorColumn;
    this.indexName = builder.indexName;
    this.distanceType = builder.distanceType;
    this.nprobes = builder.nprobes;
    this.ef = builder.ef;
    this.refineFactor = builder.refineFactor;
    this.lowerBound = builder.lowerBound;
    this.upperBound = builder.upperBound;
    this.bypassVectorIndex = builder.bypassVectorIndex;
    this.fastSearch = builder.fastSearch;
    this.prefilter = builder.prefilter;
    this.textQuery = builder.textQuery;
    this.searchColumns = immutableList(builder.searchColumns);
    this.fullTextQueryJson = builder.fullTextQueryJson;
  }

  public static Builder builder(SearchType searchType) {
    return new Builder(searchType);
  }

  static String canonicalizeDistanceType(String distanceType) {
    if (distanceType == null) {
      return null;
    }
    String normalized = distanceType.trim().toLowerCase(Locale.ROOT);
    switch (normalized) {
      case "l2":
      case "euclidean":
        return "l2";
      case "cosine":
        return "cosine";
      case "dot":
      case "ip":
      case "inner_product":
        return "dot";
      case "hamming":
        return "hamming";
      default:
        throw new IllegalArgumentException("Unsupported distance_type: " + distanceType);
    }
  }

  /** Returns a builder initialized with every field of this query. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  public List<String> getTableId() {
    return tableId;
  }

  public String getNamespaceImpl() {
    return namespaceImpl;
  }

  public Map<String, String> getNamespaceProperties() {
    return namespaceProperties;
  }

  /**
   * Read options for opening the dataset outside the driver. Only the distributed search path needs
   * them, but {@code vectorSearch} sets them before knowing which path runs, so the single
   * namespace partition carries them too. The hybrid and full-text paths leave this unset.
   */
  public LanceSparkReadOptions getReadOptions() {
    return readOptions;
  }

  /** Storage options the driver already obtained, merged into the dataset open. */
  public Map<String, String> getInitialStorageOptions() {
    return initialStorageOptions;
  }

  public List<String> getOutputColumns() {
    return outputColumns;
  }

  public Integer getK() {
    return k;
  }

  public Integer getOffset() {
    return offset;
  }

  public Long getVersion() {
    return version;
  }

  public String getFilter() {
    return filter;
  }

  public Boolean getWithRowId() {
    return withRowId;
  }

  public List<Float> getVector() {
    return vector;
  }

  /**
   * Name of the vector index a distributed search must use, or {@code null} for the default
   * selector. Only the distributed path can honour it: the namespace {@code queryTable} request has
   * no index field.
   */
  public String getIndexName() {
    return indexName;
  }

  public String getVectorColumn() {
    return vectorColumn;
  }

  public String getDistanceType() {
    return distanceType;
  }

  public Integer getNprobes() {
    return nprobes;
  }

  public Integer getEf() {
    return ef;
  }

  public Integer getRefineFactor() {
    return refineFactor;
  }

  public Float getLowerBound() {
    return lowerBound;
  }

  public Float getUpperBound() {
    return upperBound;
  }

  public Boolean getBypassVectorIndex() {
    return bypassVectorIndex;
  }

  public Boolean getFastSearch() {
    return fastSearch;
  }

  public Boolean getPrefilter() {
    return prefilter;
  }

  public String getTextQuery() {
    return textQuery;
  }

  public List<String> getSearchColumns() {
    return searchColumns;
  }

  public String getFullTextQueryJson() {
    return fullTextQueryJson;
  }

  /** Flattened batch of query vectors; empty when this is a single-vector search. */
  public List<Float> getVectors() {
    return vectors;
  }

  /** Number of query vectors in a batch, or 0 for a single-vector search. */
  public int getQueryCount() {
    return queryCount;
  }

  /** Dimension of each query vector in a batch, or 0 for a single-vector search. */
  public int getVectorDim() {
    return vectorDim;
  }

  /** True when this search carries more than one query vector. */
  public boolean isBatch() {
    return queryCount > 0;
  }

  /**
   * This batch restricted to {@code count} queries starting at {@code from}, with every other field
   * unchanged.
   *
   * <p>The queries are not renumbered: query {@code from + i} of the result is query {@code from +
   * i} of this batch, and lance-core's per-scan {@code query_index} is shifted by {@code from} when
   * the chunk is read. That is what keeps {@code query_id} equal to the input position however the
   * batch was cut up.
   */
  public LanceSearchQuery subBatch(int from, int count) {
    if (queryCount <= 0) {
      throw new IllegalStateException(
          "subBatch() needs a batch search, but this one is a single " + "vector");
    }
    if (from < 0 || count <= 0 || (long) from + count > queryCount) {
      throw new IllegalArgumentException(
          "queries ["
              + from
              + ", "
              + ((long) from + count)
              + ") are outside a batch of "
              + queryCount);
    }
    int start = from * vectorDim;
    int end = (from + count) * vectorDim;
    return toBuilder()
        .batch(vectors.subList(start, end), count, vectorDim)
        .vector(Collections.emptyList())
        .build();
  }

  public QueryTableRequest toQueryTableRequest() {
    QueryTableRequest request = new QueryTableRequest().id(tableId).k(k);
    request.vector(new QueryTableRequestVector());

    if (!outputColumns.isEmpty()) {
      request.columns(new QueryTableRequestColumns().columnNames(outputColumns));
    }
    if (offset != null) {
      request.offset(offset);
    }
    if (version != null) {
      request.version(version);
    }
    if (filter != null) {
      request.filter(filter);
    }
    if (withRowId != null) {
      request.withRowId(withRowId);
    }
    if (prefilter != null) {
      request.prefilter(prefilter);
    }

    if (searchType == SearchType.VECTOR) {
      if (isBatch()) {
        throw new IllegalArgumentException(
            "A batch search is not supported through the namespace queryTable API: its response "
                + "carries no per-query attribution. Enable "
                + "spark.sql.lance.search.distributed.enabled to run it as Spark tasks.");
      }
      request.vector(new QueryTableRequestVector().singleVector(vector));
      if (vectorColumn != null) {
        request.vectorColumn(vectorColumn);
      }
      if (distanceType != null) {
        request.distanceType(distanceType);
      }
      if (nprobes != null) {
        request.nprobes(nprobes);
      }
      if (ef != null) {
        request.ef(ef);
      }
      if (refineFactor != null) {
        request.refineFactor(refineFactor);
      }
      if (lowerBound != null) {
        request.lowerBound(lowerBound);
      }
      if (upperBound != null) {
        request.upperBound(upperBound);
      }
      if (bypassVectorIndex != null) {
        request.bypassVectorIndex(bypassVectorIndex);
      }
      if (fastSearch != null) {
        request.fastSearch(fastSearch);
      }
    } else if (fullTextQueryJson != null) {
      // Unified path: a structured FullTextQuery carried through the shared scan spec.
      request.fullTextQuery(
          new QueryTableRequestFullTextQuery()
              .structuredQuery(
                  FullTextQueryConverter.toStructuredFtsQuery(
                      FullTextQueryUtils.stringToFullTextQuery(fullTextQueryJson))));
    } else {
      StringFtsQuery stringQuery = new StringFtsQuery().query(textQuery);
      if (!searchColumns.isEmpty()) {
        stringQuery.columns(searchColumns);
      }
      request.fullTextQuery(new QueryTableRequestFullTextQuery().stringQuery(stringQuery));
    }

    return request;
  }

  private static <T> List<T> immutableList(List<T> values) {
    if (values == null || values.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(new ArrayList<>(values));
  }

  private static Map<String, String> immutableMap(Map<String, String> values) {
    if (values == null || values.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new HashMap<>(values));
  }

  public static final class Builder {
    private final SearchType searchType;
    private List<String> tableId = Collections.emptyList();
    private String namespaceImpl;
    private Map<String, String> namespaceProperties = Collections.emptyMap();
    private LanceSparkReadOptions readOptions;
    private Map<String, String> initialStorageOptions = Collections.emptyMap();
    private List<String> outputColumns = Collections.emptyList();
    private Integer k = 10;
    private Integer offset;
    private Long version;
    private String filter;
    private Boolean withRowId;
    private List<Float> vector = Collections.emptyList();
    private List<Float> vectors = Collections.emptyList();
    private int queryCount;
    private int vectorDim;
    private String vectorColumn;
    private String indexName;
    private String distanceType;
    private Integer nprobes;
    private Integer ef;
    private Integer refineFactor;
    private Float lowerBound;
    private Float upperBound;
    private Boolean bypassVectorIndex;
    private Boolean fastSearch;
    private Boolean prefilter;
    private String textQuery;
    private List<String> searchColumns = Collections.emptyList();
    private String fullTextQueryJson;

    private Builder(SearchType searchType) {
      this.searchType = searchType;
    }

    private Builder(LanceSearchQuery query) {
      this.searchType = query.searchType;
      this.tableId = query.tableId;
      this.namespaceImpl = query.namespaceImpl;
      this.namespaceProperties = query.namespaceProperties;
      this.readOptions = query.readOptions;
      this.initialStorageOptions = query.initialStorageOptions;
      this.outputColumns = query.outputColumns;
      this.k = query.k;
      this.offset = query.offset;
      this.version = query.version;
      this.filter = query.filter;
      this.withRowId = query.withRowId;
      this.vector = query.vector;
      this.vectors = query.vectors;
      this.queryCount = query.queryCount;
      this.vectorDim = query.vectorDim;
      this.vectorColumn = query.vectorColumn;
      this.indexName = query.indexName;
      this.distanceType = query.distanceType;
      this.nprobes = query.nprobes;
      this.ef = query.ef;
      this.refineFactor = query.refineFactor;
      this.lowerBound = query.lowerBound;
      this.upperBound = query.upperBound;
      this.bypassVectorIndex = query.bypassVectorIndex;
      this.fastSearch = query.fastSearch;
      this.prefilter = query.prefilter;
      this.textQuery = query.textQuery;
      this.searchColumns = query.searchColumns;
      this.fullTextQueryJson = query.fullTextQueryJson;
    }

    public Builder tableId(List<String> tableId) {
      this.tableId = tableId;
      return this;
    }

    public Builder namespaceImpl(String namespaceImpl) {
      this.namespaceImpl = namespaceImpl;
      return this;
    }

    public Builder namespaceProperties(Map<String, String> namespaceProperties) {
      this.namespaceProperties = namespaceProperties;
      return this;
    }

    /** Required for distributed search, which opens the dataset itself; unused otherwise. */
    public Builder readOptions(LanceSparkReadOptions readOptions) {
      this.readOptions = readOptions;
      return this;
    }

    public Builder initialStorageOptions(Map<String, String> initialStorageOptions) {
      this.initialStorageOptions = initialStorageOptions;
      return this;
    }

    public Builder outputColumns(List<String> outputColumns) {
      this.outputColumns = outputColumns;
      return this;
    }

    public Builder topK(Integer k) {
      this.k = k;
      return this;
    }

    public Builder offset(Integer offset) {
      this.offset = offset;
      return this;
    }

    public Builder version(Long version) {
      this.version = version;
      return this;
    }

    public Builder filter(String filter) {
      this.filter = filter;
      return this;
    }

    public Builder withRowId(Boolean withRowId) {
      this.withRowId = withRowId;
      return this;
    }

    public Builder vector(List<Float> vector) {
      this.vector = vector;
      return this;
    }

    /**
     * A batch of query vectors, all of the same dimension, given as {@code queries} rows of {@code
     * dimension} floats each. The batch is stored flattened, which is the layout lance-core takes
     * and the one a task has to rebuild only once.
     */
    public Builder vectors(List<List<Float>> queries) {
      if (queries == null || queries.isEmpty()) {
        this.vectors = new ArrayList<>();
        this.queryCount = 0;
        this.vectorDim = 0;
        return this;
      }
      int dimension = queries.get(0).size();
      List<Float> flat = new ArrayList<>(queries.size() * dimension);
      for (int row = 0; row < queries.size(); row++) {
        List<Float> query = queries.get(row);
        if (query.size() != dimension) {
          throw new IllegalArgumentException(
              "Every query vector must have dimension "
                  + dimension
                  + ", but query "
                  + row
                  + " has "
                  + query.size());
        }
        flat.addAll(query);
      }
      this.vectors = flat;
      this.queryCount = queries.size();
      this.vectorDim = dimension;
      return this;
    }

    /**
     * Sets a batch directly from the flattened {@code queries x dimension} matrix, for a chunk of
     * an existing batch. The list is copied, so a view such as {@code subList} is safe here.
     */
    public Builder batch(List<Float> flat, int queries, int dimension) {
      if (flat == null || queries <= 0 || dimension <= 0) {
        throw new IllegalArgumentException(
            "a batch needs queries * dimension floats, got " + (flat == null ? 0 : flat.size()));
      }
      if (flat.size() != (long) queries * dimension) {
        throw new IllegalArgumentException(
            "a batch of "
                + queries
                + " queries of dimension "
                + dimension
                + " needs "
                + ((long) queries * dimension)
                + " floats, got "
                + flat.size());
      }
      this.vectors = new ArrayList<>(flat);
      this.queryCount = queries;
      this.vectorDim = dimension;
      return this;
    }

    public Builder vectorColumn(String vectorColumn) {
      this.vectorColumn = vectorColumn;
      return this;
    }

    /** Names the vector index to search. Only the distributed path can honour it. */
    public Builder indexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    public Builder distanceType(String distanceType) {
      this.distanceType = canonicalizeDistanceType(distanceType);
      return this;
    }

    public Builder nprobes(Integer nprobes) {
      this.nprobes = nprobes;
      return this;
    }

    public Builder ef(Integer ef) {
      this.ef = ef;
      return this;
    }

    public Builder refineFactor(Integer refineFactor) {
      this.refineFactor = refineFactor;
      return this;
    }

    public Builder lowerBound(Float lowerBound) {
      this.lowerBound = lowerBound;
      return this;
    }

    public Builder upperBound(Float upperBound) {
      this.upperBound = upperBound;
      return this;
    }

    public Builder bypassVectorIndex(Boolean bypassVectorIndex) {
      this.bypassVectorIndex = bypassVectorIndex;
      return this;
    }

    public Builder fastSearch(Boolean fastSearch) {
      this.fastSearch = fastSearch;
      return this;
    }

    public Builder prefilter(Boolean prefilter) {
      this.prefilter = prefilter;
      return this;
    }

    public Builder textQuery(String textQuery) {
      this.textQuery = textQuery;
      return this;
    }

    public Builder searchColumns(List<String> searchColumns) {
      this.searchColumns = searchColumns;
      return this;
    }

    /** Serialized structured FullTextQuery for the unified scan path (see FullTextQueryUtils). */
    public Builder fullTextQueryJson(String fullTextQueryJson) {
      this.fullTextQueryJson = fullTextQueryJson;
      return this;
    }

    public LanceSearchQuery build() {
      if (searchType == null) {
        throw new IllegalArgumentException("search type is required");
      }
      if (tableId == null || tableId.isEmpty()) {
        throw new IllegalArgumentException("table id is required");
      }
      if (namespaceImpl == null || namespaceImpl.isEmpty()) {
        throw new IllegalArgumentException("namespace implementation is required");
      }
      if (k != null && k <= 0) {
        throw new IllegalArgumentException("k must be positive");
      }
      if (offset != null && offset < 0) {
        throw new IllegalArgumentException("offset must be non-negative");
      }
      if (Boolean.TRUE.equals(bypassVectorIndex) && Boolean.TRUE.equals(fastSearch)) {
        throw new IllegalArgumentException(
            "bypass_vector_index and fast_search cannot both be true");
      }
      if (searchType == SearchType.VECTOR) {
        boolean single = vector != null && !vector.isEmpty();
        boolean batch = queryCount > 0;
        if (single && batch) {
          throw new IllegalArgumentException(
              "Specify either query_vector or a batch of queries, not both");
        }
        if (!single && !batch) {
          throw new IllegalArgumentException("query_vector is required");
        }
        if (batch && (vectors.size() != (long) queryCount * vectorDim)) {
          throw new IllegalArgumentException(
              "A batch search needs queries * dimension vectors, got "
                  + vectors.size()
                  + " for "
                  + queryCount
                  + " queries of dimension "
                  + vectorDim);
        }
      }
      if (searchType == SearchType.FULL_TEXT
          && (textQuery == null || textQuery.isEmpty())
          && fullTextQueryJson == null) {
        throw new IllegalArgumentException("query is required");
      }
      return new LanceSearchQuery(this);
    }
  }
}
