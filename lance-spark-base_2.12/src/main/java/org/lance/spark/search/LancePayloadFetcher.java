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
import org.lance.spark.internal.ExecutorNamespace;
import org.lance.spark.utils.Utils;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Reads the payload columns of already-ranked rows by stable row id, for the second phase of a
 * deferred vector search.
 *
 * <p>The first phase runs one narrow search task per index segment (or per unindexed fragment) and
 * returns only {@code _distance} and {@code _rowid}. Spark ranks those candidates globally, and
 * this fetch materializes the payload for the winners only, through {@code Dataset.takeRows} on the
 * same pinned snapshot. Deferring the payload drops the per-unit copies of it, at the cost of one
 * extra random-access read.
 *
 * <p>Row ids are never matched to rows by array position. lance-core returns the requested rows in
 * the requested order, duplicates included, but it <em>silently drops</em> a row id that is not in
 * the snapshot - so a positional mapping would shift every later row onto the wrong payload. {@code
 * _rowid} is therefore requested next to the payload and every returned row id is compared with the
 * one asked for; a mismatch fails the task instead of producing wrong rows.
 *
 * <p>Request size is bounded in bytes, not only in rows: {@code takeRows} returns one complete
 * Arrow IPC {@code byte[]} per call, so a request is sized from the dataset's real Arrow schema (a
 * 2560 dimensional vector column is 10 KiB per row, not the 1 KiB a Spark-ArrayType estimate
 * suggests). The estimate is per requested field; a variable-width column is charged a nominal
 * width and the configured row count remains the hard cap.
 */
public final class LancePayloadFetcher implements Serializable {
  private static final long serialVersionUID = 748193927461839271L;

  /** The column {@link #fetch} asks for in addition to the payload, to verify the mapping. */
  public static final String ROW_ID_COLUMN = "_rowid";

  /**
   * Upper bound on the payload bytes one {@code takeRows} request asks lance-core for. The native
   * call builds the whole response as one {@code byte[]}, so this is the memory bound of a fetch
   * request, not a tuning knob.
   */
  public static final long ChunkBytes = 16L * 1024 * 1024;

  /** Nominal width charged for a variable-width payload column when sizing a request. */
  private static final long VariableWidthNominal = 128L;

  /** Nominal width charged for a nested or collection payload column when sizing a request. */
  private static final long NestedNominal = 1024L;

  private final LanceFetchPayloadSource source;
  private final StructType schema;
  private final List<String> payloadColumns;
  private final List<String> narrowColumns;
  private final int maxRowsPerRequest;
  private final LancePayloadMetrics metrics;

  /** Schema positions and fields of the narrow (ranking) columns, in schema order. */
  private final int[] narrowPositions;

  private final StructField[] narrowFields;

  /**
   * @param source the pinned read options, credentials and namespace used to reopen the dataset
   * @param schema the full output schema, in the order the caller declared it
   * @param payloadColumns the columns of {@code schema} this fetcher materializes, in schema order
   * @param narrowColumns the internal columns the search produced, in schema order; matching is by
   *     name against an explicit list, so a real table column called {@code query_index} is treated
   *     as payload
   * @param maxRowsPerRequest cap on rows per {@code takeRows} request; the byte budget can lower it
   */
  public LancePayloadFetcher(
      LanceFetchPayloadSource source,
      StructType schema,
      List<String> payloadColumns,
      List<String> narrowColumns,
      int maxRowsPerRequest,
      LancePayloadMetrics metrics) {
    this.source = source;
    this.schema = schema;
    this.payloadColumns = Collections.unmodifiableList(new ArrayList<>(payloadColumns));
    this.narrowColumns = Collections.unmodifiableList(new ArrayList<>(narrowColumns));
    this.maxRowsPerRequest = Math.max(maxRowsPerRequest, 1);
    this.metrics = metrics;

    StructField[] fields = schema.fields();
    List<Integer> positions = new ArrayList<>();
    List<StructField> narrow = new ArrayList<>();
    for (int position = 0; position < fields.length; position++) {
      if (isNarrow(fields[position].name())) {
        positions.add(position);
        narrow.add(fields[position]);
      }
    }
    if (positions.size() != narrowColumns.size()) {
      throw new IllegalStateException(
          "deferred payload fetch got narrow columns "
              + narrowColumns
              + " but the schema "
              + schema
              + " contains "
              + positions.size()
              + " of them");
    }
    this.narrowPositions = new int[positions.size()];
    for (int i = 0; i < positions.size(); i++) {
      this.narrowPositions[i] = positions.get(i);
    }
    this.narrowFields = narrow.toArray(new StructField[0]);
  }

  public boolean hasPayload() {
    return !payloadColumns.isEmpty();
  }

  /**
   * Rows one {@code takeRows} request may ask for, given the dataset's real Arrow schema: the
   * configured cap, lowered until the estimated response fits {@link #ChunkBytes}.
   */
  public static int rowsPerRequest(Schema arrowSchema, List<String> payloadColumns, int cap) {
    long width = 8L; // the _rowid column the fetch adds to verify the mapping
    for (String name : payloadColumns) {
      width += fieldWidth(arrowSchema == null ? null : arrowSchema.findField(name));
    }
    long byBytes = Math.max(1L, ChunkBytes / Math.max(width, 1L));
    return (int) Math.max(1L, Math.min(Math.max(cap, 1), byBytes));
  }

  private static long fieldWidth(Field field) {
    if (field == null) {
      return NestedNominal;
    }
    ArrowType type = field.getType();
    if (type instanceof ArrowType.Int) {
      return Math.max(1L, ((ArrowType.Int) type).getBitWidth() / 8L);
    }
    if (type instanceof ArrowType.FloatingPoint) {
      switch (((ArrowType.FloatingPoint) type).getPrecision()) {
        case HALF:
          return 2L;
        case SINGLE:
          return 4L;
        default:
          return 8L;
      }
    }
    if (type instanceof ArrowType.Bool) {
      return 1L;
    }
    if (type instanceof ArrowType.FixedSizeBinary) {
      return Math.max(1L, ((ArrowType.FixedSizeBinary) type).getByteWidth());
    }
    if (type instanceof ArrowType.Decimal) {
      return 16L;
    }
    if (type instanceof ArrowType.Date) {
      return ((ArrowType.Date) type).getUnit() == DateUnit.DAY ? 4L : 8L;
    }
    if (type instanceof ArrowType.Timestamp
        || type instanceof ArrowType.Time
        || type instanceof ArrowType.Duration) {
      return 8L;
    }
    if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.Binary) {
      return VariableWidthNominal;
    }
    if (type instanceof ArrowType.FixedSizeList) {
      long size = ((ArrowType.FixedSizeList) type).getListSize();
      long element =
          field.getChildren().isEmpty()
              ? VariableWidthNominal
              : fieldWidth(field.getChildren().get(0));
      return Math.max(1L, size * element);
    }
    return NestedNominal;
  }

  /**
   * Returns the payload for one chunk of winners, as columnar batches that carry every column of
   * {@code schema}, in the requested order. Requests are split so one response stays bounded; the
   * dataset is opened once per chunk and closed when the iterator is exhausted.
   */
  public Iterator<ColumnarBatch> fetch(
      final long[] rowIds, final float[] distances, final int[] queryIndexes) {
    if (rowIds.length != distances.length || !isQueryIndexConsistent(rowIds, queryIndexes)) {
      throw new IllegalStateException(
          "deferred payload fetch received mismatched winner arrays: rowIds="
              + rowIds.length
              + ", distances="
              + distances.length
              + ", queryIndexes="
              + queryIndexes.length);
    }
    if (!hasPayload()) {
      // A plan whose payload columns were all pruned away still needs the narrow columns, and
      // opening the dataset again would only add an unnecessary snapshot read.
      return narrowOnly(rowIds, distances, queryIndexes);
    }

    final Dataset dataset;
    final ExecutorNamespace executorNamespace;
    try {
      // Same credential handling as the search tasks: a long fetch must not run on an STS token
      // that expired while the first phase was scanning.
      executorNamespace =
          ExecutorNamespace.acquire(
              source.readOptions(), source.namespaceImpl(), source.namespaceProperties());
      dataset =
          Utils.openDatasetBuilder(source.readOptions())
              .initialStorageOptions(source.initialStorageOptions())
              .build();
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Failed to open the dataset for the deferred search payload of "
              + rowIds.length
              + " row ids",
          e);
    }

    final int requests = rowsPerRequest(dataset.getSchema(), payloadColumns, maxRowsPerRequest);
    final List<String> requestedColumns = requestedColumns();

    return new Iterator<ColumnarBatch>() {
      private int windowStart;
      private Iterator<ColumnarBatch> window = Collections.emptyIterator();
      private boolean closed;

      @Override
      public boolean hasNext() {
        while (!window.hasNext()) {
          if (windowStart >= rowIds.length) {
            close();
            return false;
          }
          int count = Math.min(requests, rowIds.length - windowStart);
          window =
              request(
                  rowIds, distances, queryIndexes, windowStart, count, requestedColumns, dataset);
          windowStart += count;
        }
        return true;
      }

      @Override
      public ColumnarBatch next() {
        if (!hasNext()) {
          throw new NoSuchElementException("no more deferred search payload batches");
        }
        return window.next();
      }

      private void close() {
        if (closed) {
          return;
        }
        closed = true;
        try {
          dataset.close();
        } finally {
          executorNamespace.close();
        }
      }
    };
  }

  /** One {@code takeRows} request: reads {@code count} row ids starting at {@code start}. */
  private Iterator<ColumnarBatch> request(
      final long[] rowIds,
      final float[] distances,
      final int[] queryIndexes,
      final int start,
      final int count,
      final List<String> requestedColumns,
      final Dataset dataset) {
    final List<Long> ids = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      ids.add(rowIds[start + i]);
    }
    final ArrowReader reader;
    try {
      reader = dataset.takeRows(ids, requestedColumns);
    } catch (IOException | RuntimeException e) {
      throw new IllegalStateException(
          "Failed to read deferred search payload for " + count + " row ids starting at " + start,
          e);
    }
    if (metrics != null) {
      metrics.add(1, 0L, 0L);
    }
    return new Iterator<ColumnarBatch>() {
      private int position;
      private ColumnarBatch next;
      private boolean closed;

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }
        try {
          while (next == null && reader.loadNextBatch()) {
            next = toColumnarBatch(reader.getVectorSchemaRoot());
          }
        } catch (IOException e) {
          close();
          throw new IllegalStateException("Failed to read deferred search payload", e);
        }
        if (next == null) {
          if (position != count) {
            String message =
                "Deferred payload fetch returned "
                    + position
                    + " rows for "
                    + count
                    + " row ids; the search and the fetch may be reading different snapshots";
            close();
            throw new IllegalStateException(message);
          }
          close();
          return false;
        }
        return true;
      }

      @Override
      public ColumnarBatch next() {
        if (!hasNext()) {
          throw new NoSuchElementException("no more deferred search payload batches");
        }
        ColumnarBatch batch = next;
        next = null;
        return batch;
      }

      /**
       * Converts one Arrow batch of payload plus {@code _rowid} into a Spark batch whose columns
       * follow {@code schema}. Returned row ids are checked against the requested ones here, so a
       * payload can never be attached to the wrong row.
       */
      private ColumnarBatch toColumnarBatch(VectorSchemaRoot root) {
        int rows = root.getRowCount();
        if (rows == 0) {
          return new ColumnarBatch(new ColumnVector[0], 0);
        }
        if (position + rows > count) {
          close();
          throw new IllegalStateException(
              "Deferred payload fetch returned more rows than requested: "
                  + (position + rows)
                  + " > "
                  + count);
        }
        Map<String, FieldVector> payloadFields = new HashMap<>();
        FieldVector returnedRowIds = null;
        for (FieldVector vector : root.getFieldVectors()) {
          if (ROW_ID_COLUMN.equals(vector.getName())) {
            returnedRowIds = vector;
          } else {
            payloadFields.put(vector.getName(), vector);
          }
        }
        if (returnedRowIds == null) {
          close();
          throw new IllegalStateException(
              "Deferred payload fetch did not return " + ROW_ID_COLUMN + " to verify row identity");
        }
        long decodedBytes = 0L;
        for (int row = 0; row < rows; row++) {
          long expected = rowIds[start + position + row];
          long actual = ((Number) returnedRowIds.getObject(row)).longValue();
          if (expected != actual) {
            close();
            throw new IllegalStateException(
                "Deferred payload fetch returned row id "
                    + actual
                    + " where "
                    + expected
                    + " was requested (offset "
                    + (start + position + row)
                    + "); refusing to attach payload by array position");
          }
        }

        OnHeapColumnVector[] narrow = OnHeapColumnVector.allocateColumns(rows, narrowFields);
        for (int i = 0; i < narrow.length; i++) {
          String name = narrowFields[i].name();
          switch (name) {
            case LanceSearchProjection.DISTANCE_COLUMN:
              for (int row = 0; row < rows; row++) {
                narrow[i].putFloat(row, distances[start + position + row]);
              }
              break;
            case ROW_ID_COLUMN:
              for (int row = 0; row < rows; row++) {
                narrow[i].putLong(row, rowIds[start + position + row]);
              }
              break;
            case LanceSearchProjection.QUERY_INDEX:
              for (int row = 0; row < rows; row++) {
                narrow[i].putInt(row, queryIndexes[start + position + row]);
              }
              break;
            default:
              close();
              throw new IllegalStateException(
                  "Column '" + name + "' is not a narrow search column");
          }
        }

        StructField[] fields = schema.fields();
        ColumnVector[] vectors = new ColumnVector[fields.length];
        for (int i = 0; i < narrow.length; i++) {
          vectors[narrowPositions[i]] = narrow[i];
        }
        for (int column = 0; column < fields.length; column++) {
          if (vectors[column] != null) {
            continue;
          }
          String name = fields[column].name();
          FieldVector field = payloadFields.get(name);
          if (field == null) {
            close();
            throw new IllegalStateException(
                "Deferred payload fetch did not return column '" + name + "'");
          }
          decodedBytes += field.getBufferSize();
          vectors[column] = new LanceArrowColumnVector(field, false);
        }
        ColumnarBatch sparkBatch = new ColumnarBatch(vectors, rows);
        position += rows;
        if (metrics != null) {
          metrics.add(0, rows, decodedBytes);
        }
        return sparkBatch;
      }

      private void close() {
        if (closed) {
          return;
        }
        closed = true;
        try {
          reader.close();
        } catch (IOException ignored) {
          // best effort: the response bytes are already in memory
        }
      }
    };
  }

  private boolean isNarrow(String name) {
    return narrowColumns.contains(name);
  }

  private boolean isQueryIndexConsistent(long[] rowIds, int[] queryIndexes) {
    boolean batch = narrowColumns.contains(LanceSearchProjection.QUERY_INDEX);
    return !batch || rowIds.length == queryIndexes.length;
  }

  private List<String> requestedColumns() {
    List<String> columns = new ArrayList<>(payloadColumns);
    if (!columns.contains(ROW_ID_COLUMN)) {
      columns.add(ROW_ID_COLUMN);
    }
    return columns;
  }

  /** Emits only the narrow columns, for a plan whose payload columns were all pruned away. */
  private Iterator<ColumnarBatch> narrowOnly(
      final long[] rowIds, final float[] distances, final int[] queryIndexes) {
    final int rows = rowIds.length;
    final StructField[] fields = schema.fields();
    final ColumnVector[] vectors = new ColumnVector[fields.length];
    OnHeapColumnVector[] narrow = OnHeapColumnVector.allocateColumns(rows, narrowFields);
    for (int i = 0; i < narrow.length; i++) {
      String name = narrowFields[i].name();
      switch (name) {
        case LanceSearchProjection.DISTANCE_COLUMN:
          for (int row = 0; row < rows; row++) {
            narrow[i].putFloat(row, distances[row]);
          }
          break;
        case ROW_ID_COLUMN:
          for (int row = 0; row < rows; row++) {
            narrow[i].putLong(row, rowIds[row]);
          }
          break;
        case LanceSearchProjection.QUERY_INDEX:
          for (int row = 0; row < rows; row++) {
            narrow[i].putInt(row, queryIndexes[row]);
          }
          break;
        default:
          throw new IllegalStateException("Column '" + name + "' is not a narrow search column");
      }
      vectors[narrowPositions[i]] = narrow[i];
    }
    final ColumnarBatch batch = new ColumnarBatch(vectors, rows);
    return new Iterator<ColumnarBatch>() {
      private boolean consumed;

      @Override
      public boolean hasNext() {
        return !consumed;
      }

      @Override
      public ColumnarBatch next() {
        if (consumed) {
          throw new NoSuchElementException("no more narrow search batches");
        }
        consumed = true;
        return batch;
      }
    };
  }

  List<String> getPayloadColumns() {
    return payloadColumns;
  }
}
