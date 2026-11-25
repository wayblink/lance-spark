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
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
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
 * one asked for; a mismatch fails the task instead of producing wrong rows. The fetch is chunked by
 * the caller so the Arrow IPC buffer one call builds stays bounded.
 */
public final class LancePayloadFetcher implements Serializable {
  private static final long serialVersionUID = 748193927461839271L;

  /** The column {@link #fetch} asks for in addition to the payload, to verify the mapping. */
  public static final String ROW_ID_COLUMN = "_rowid";

  private final LanceSparkReadOptions readOptions;
  private final Map<String, String> initialStorageOptions;
  private final StructType schema;
  private final List<String> payloadColumns;

  /** Schema positions and fields of the narrow (ranking) columns, in schema order. */
  private final int[] narrowPositions;

  private final StructField[] narrowFields;

  /**
   * @param readOptions the options the search planned with; they must carry the version the search
   *     ran on, not "latest"
   * @param schema the full output schema, in the order the caller declared it
   * @param payloadColumns the columns of {@code schema} this fetcher materializes, in schema order
   */
  public LancePayloadFetcher(
      LanceSparkReadOptions readOptions,
      Map<String, String> initialStorageOptions,
      StructType schema,
      List<String> payloadColumns) {
    this.readOptions = readOptions;
    this.initialStorageOptions =
        initialStorageOptions == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(initialStorageOptions));
    this.schema = schema;
    this.payloadColumns = Collections.unmodifiableList(new ArrayList<>(payloadColumns));

    StructField[] fields = schema.fields();
    List<Integer> positions = new ArrayList<>();
    List<StructField> narrow = new ArrayList<>();
    for (int position = 0; position < fields.length; position++) {
      if (isNarrowColumn(fields[position].name())) {
        positions.add(position);
        narrow.add(fields[position]);
      }
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
   * Returns the payload for one chunk of winners, as columnar batches that carry every column of
   * {@code schema}. Batches are produced lazily so at most one Arrow batch is held at a time.
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

    Dataset opened = null;
    ArrowReader arrowReader = null;
    try {
      opened =
          Utils.openDatasetBuilder(readOptions)
              .initialStorageOptions(initialStorageOptions)
              .build();
      arrowReader = opened.takeRows(boxedRowIds(rowIds), requestedColumns());
    } catch (IOException | RuntimeException e) {
      closeQuietly(arrowReader, opened);
      throw new IllegalStateException(
          "Failed to read deferred search payload for " + rowIds.length + " row ids", e);
    }

    final Dataset dataset = opened;
    final ArrowReader reader = arrowReader;
    final int[] narrowPositions = this.narrowPositions;
    final StructField[] narrowFields = this.narrowFields;
    final StructField[] fields = schema.fields();

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
          if (position != rowIds.length) {
            String message =
                "Deferred payload fetch returned "
                    + position
                    + " rows for "
                    + rowIds.length
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
        if (position + rows > rowIds.length) {
          close();
          throw new IllegalStateException(
              "Deferred payload fetch returned more rows than requested: "
                  + (position + rows)
                  + " > "
                  + rowIds.length);
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
        for (int row = 0; row < rows; row++) {
          long expected = rowIds[position + row];
          long actual = ((Number) returnedRowIds.getObject(row)).longValue();
          if (expected != actual) {
            close();
            throw new IllegalStateException(
                "Deferred payload fetch returned row id "
                    + actual
                    + " where "
                    + expected
                    + " was requested (offset "
                    + (position + row)
                    + "); refusing to attach payload by array position");
          }
        }

        OnHeapColumnVector[] narrow = OnHeapColumnVector.allocateColumns(rows, narrowFields);
        for (int i = 0; i < narrow.length; i++) {
          fillNarrow(narrow[i], narrowFields[i].name(), rows);
        }

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
          vectors[column] = new LanceArrowColumnVector(field, false);
        }
        ColumnarBatch sparkBatch = new ColumnarBatch(vectors, rows);
        position += rows;
        return sparkBatch;
      }

      private void fillNarrow(OnHeapColumnVector vector, String name, int rows) {
        switch (name) {
          case LanceSearchProjection.DISTANCE_COLUMN:
            for (int row = 0; row < rows; row++) {
              vector.putFloat(row, distances[position + row]);
            }
            break;
          case ROW_ID_COLUMN:
            for (int row = 0; row < rows; row++) {
              vector.putLong(row, rowIds[position + row]);
            }
            break;
          case LanceSearchProjection.QUERY_INDEX:
            for (int row = 0; row < rows; row++) {
              vector.putInt(row, queryIndexes[position + row]);
            }
            break;
          default:
            close();
            throw new IllegalStateException("Column '" + name + "' is not a narrow search column");
        }
      }

      private void close() {
        if (closed) {
          return;
        }
        closed = true;
        closeQuietly(reader, dataset);
      }
    };
  }

  private boolean isNarrowColumn(String name) {
    if (LanceSearchProjection.DISTANCE_COLUMN.equals(name) || ROW_ID_COLUMN.equals(name)) {
      return true;
    }
    return LanceSearchProjection.QUERY_INDEX.equals(name);
  }

  private boolean isQueryIndexConsistent(long[] rowIds, int[] queryIndexes) {
    boolean batch = false;
    for (StructField field : schema.fields()) {
      if (LanceSearchProjection.QUERY_INDEX.equals(field.name())) {
        batch = true;
      }
    }
    return !batch || rowIds.length == queryIndexes.length;
  }

  private List<String> requestedColumns() {
    List<String> columns = new ArrayList<>(payloadColumns);
    if (!columns.contains(ROW_ID_COLUMN)) {
      columns.add(ROW_ID_COLUMN);
    }
    return columns;
  }

  private static List<Long> boxedRowIds(long[] rowIds) {
    List<Long> boxed = new ArrayList<>(rowIds.length);
    for (long rowId : rowIds) {
      boxed.add(rowId);
    }
    return boxed;
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

  private static void closeQuietly(ArrowReader reader, Dataset dataset) {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException ignored) {
        // best effort: the response bytes are already in memory
      }
    }
    if (dataset != null) {
      dataset.close();
    }
  }
}
