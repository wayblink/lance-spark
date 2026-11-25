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
import org.lance.ReadOptions;
import org.lance.WriteParams;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceRuntime;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for the lance-core 14.0.3 {@code Dataset.takeRows} semantics the deferred payload
 * fetch is built on (card #3258).
 *
 * <p>Each case here was observed on a real local Lance dataset before the connector started using
 * the call; the assertions are the contract the fetch relies on:
 *
 * <ul>
 *   <li>rows come back in the requested order, duplicates included, so a chunk can be zipped with
 *       its row ids;
 *   <li>a row id that is not in the snapshot is <em>silently dropped</em> - not an error, not a
 *       null - which is why the fetch also requests {@code _rowid} and verifies every returned id
 *       instead of trusting the position;
 *   <li>a row deleted in a newer version is still returned when the dataset is opened at the
 *       version the search ran on, and dropped at latest, which is why the fetch must reuse the
 *       pinned snapshot;
 *   <li>{@code _rowid} can be requested next to the payload, which is what makes that verification
 *       possible.
 * </ul>
 *
 * <p>Two further observations are documented rather than asserted, because they cannot be expressed
 * as a JUnit assertion in this JVM: an unknown column name in the projection aborts the process
 * inside the native layer ({@code FieldNotFound} panic, no Java exception), so the caller must only
 * ask for columns it validated; and the native call returns one complete Arrow IPC {@code byte[]}
 * before the Java side wraps it in an {@code ArrowReader}, so the fetch has to be chunked.
 */
class TakeRowsSemanticsTest {
  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
              new Field(
                  "score",
                  FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                  null)));

  @TempDir Path tempDir;

  private String uri;

  @BeforeEach
  void createDataset() throws Exception {
    uri = tempDir.resolve("take-rows.lance").toString();
    writeBatch(WriteParams.WriteMode.CREATE, 0, 10);
  }

  @Test
  void returnsRowsInRequestedOrderIncludingOutOfOrderInput() throws Exception {
    try (Dataset dataset = open()) {
      List<Long> rowIds = new ArrayList<>(readRowIds(dataset));
      Collections.reverse(rowIds);

      List<String> names = readNames(dataset.takeRows(rowIds, Arrays.asList("name")));

      List<String> expected = new ArrayList<>();
      for (long rowId : rowIds) {
        expected.add("n" + rowId);
      }
      assertEquals(expected, names, "takeRows must answer in the requested order");
    }
  }

  @Test
  void returnsDuplicatesOncePerOccurrence() throws Exception {
    try (Dataset dataset = open()) {
      List<Long> rowIds = readRowIds(dataset);
      List<Long> requested =
          new ArrayList<>(Arrays.asList(rowIds.get(0), rowIds.get(1), rowIds.get(0)));

      List<Integer> ids = readIds(dataset.takeRows(requested, Arrays.asList("id")));

      assertEquals(
          Arrays.asList(0, 1, 0), ids, "a repeated row id must come back once per occurrence");
    }
  }

  @Test
  void silentlyDropsRowIdsMissingFromTheSnapshot() throws Exception {
    try (Dataset dataset = open()) {
      List<Long> missing = Collections.singletonList(999_999L);

      // No exception, no null row: the requested row simply is not in the result. A positional
      // payload mapping would shift every following row, so the fetch verifies `_rowid` instead.
      assertEquals(
          Collections.emptyList(), readIds(dataset.takeRows(missing, Arrays.asList("id"))));
    }
  }

  @Test
  void returnsDeletedRowAtThePinnedVersionAndDropsItAtLatest() throws Exception {
    long pinnedVersion;
    long deletedRowId;
    try (Dataset dataset = open()) {
      pinnedVersion = dataset.version();
      deletedRowId = readRowIds(dataset).get(3);
    }
    try (Dataset writer = open()) {
      writer.delete("id = 3");
    }

    try (Dataset pinned =
        Dataset.open(uri, new ReadOptions.Builder().setVersion(pinnedVersion).build())) {
      assertEquals(
          Collections.singletonList(3),
          readIds(pinned.takeRows(Collections.singletonList(deletedRowId), Arrays.asList("id"))),
          "a pinned fetch must still see the row the search saw");
    }
    try (Dataset latest = open()) {
      assertEquals(
          Collections.emptyList(),
          readIds(latest.takeRows(Collections.singletonList(deletedRowId), Arrays.asList("id"))),
          "at latest the deleted row is silently gone");
    }
  }

  @Test
  void exposesRowIdInTheProjectionToVerifyIdentity() throws Exception {
    try (Dataset dataset = open()) {
      List<Long> requested = readRowIds(dataset).subList(0, 3);

      List<Long> returned = new ArrayList<>();
      try (ArrowReader reader =
          dataset.takeRows(requested, Arrays.asList(LancePayloadFetcher.ROW_ID_COLUMN, "name"))) {
        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          FieldVector rowIds = root.getVector(LancePayloadFetcher.ROW_ID_COLUMN);
          for (int row = 0; row < root.getRowCount(); row++) {
            returned.add(((Number) rowIds.getObject(row)).longValue());
          }
        }
      }

      assertEquals(requested, returned, "_rowid makes the payload mapping verifiable");
    }
  }

  @Test
  void chunkedFetchesMatchOneCall() throws Exception {
    try (Dataset dataset = open()) {
      List<Long> rowIds = readRowIds(dataset);

      List<Integer> oneCall = readIds(dataset.takeRows(rowIds, Arrays.asList("id")));
      List<Integer> chunked = new ArrayList<>();
      for (int start = 0; start < rowIds.size(); start += 4) {
        List<Long> chunk = rowIds.subList(start, Math.min(start + 4, rowIds.size()));
        chunked.addAll(readIds(dataset.takeRows(chunk, Arrays.asList("id"))));
      }

      assertEquals(oneCall, chunked, "a bounded chunk loop must return the same rows");
    }
  }

  @Test
  void stableRowIdsSurviveAnAppend() throws Exception {
    List<Long> before;
    try (Dataset dataset = open()) {
      assertTrue(dataset.hasStableRowIds(), "the fixture is written with stable row ids");
      before = readRowIds(dataset);
    }

    writeBatch(WriteParams.WriteMode.APPEND, 10, 5);

    try (Dataset dataset = open()) {
      List<Long> after = readRowIds(dataset);
      assertEquals(
          before, after.subList(0, before.size()), "an append must not renumber the existing rows");
      assertEquals(15, after.size());
      assertEquals(
          Arrays.asList(10, 11, 12, 13, 14),
          readIds(
              dataset.takeRows(after.subList(before.size(), after.size()), Arrays.asList("id"))));
    }
  }

  @Test
  void rejectsEmptyAndNullRowIdLists() throws Exception {
    try (Dataset dataset = open()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> dataset.takeRows(Collections.emptyList(), Arrays.asList("id")));
      assertThrows(
          IllegalArgumentException.class, () -> dataset.takeRows(null, Arrays.asList("id")));
    }
  }

  @Test
  void returnsEveryRowWhenAllRowIdsAreRequested() throws Exception {
    try (Dataset dataset = open()) {
      List<Long> rowIds = readRowIds(dataset);
      List<Integer> ids = readIds(dataset.takeRows(rowIds, Arrays.asList("id")));
      assertEquals(10, ids.size());
      assertFalse(ids.contains(-1));
    }
  }

  private Dataset open() {
    return Dataset.open(uri, new ReadOptions.Builder().build());
  }

  private void writeBatch(WriteParams.WriteMode mode, int startId, int count) throws Exception {
    BufferAllocator allocator = LanceRuntime.allocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      Float4Vector scoreVec = (Float4Vector) root.getVector("score");
      for (int i = 0; i < count; i++) {
        int id = startId + i;
        idVec.setSafe(i, id);
        nameVec.setSafe(i, ("n" + id).getBytes(StandardCharsets.UTF_8));
        scoreVec.setSafe(i, id * 1.5f);
      }
      root.setRowCount(count);

      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, bytes)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      try (ArrowStreamReader reader =
              new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator);
          ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, reader, stream);
        Dataset.write().stream(stream)
            .uri(uri)
            .mode(mode)
            .enableStableRowIds(true)
            .execute()
            .close();
      }
    }
  }

  private static List<Long> readRowIds(Dataset dataset) throws Exception {
    List<Long> rowIds = new ArrayList<>();
    try (LanceScanner scanner = dataset.newScan(new ScanOptions.Builder().withRowId(true).build());
        ArrowReader reader = scanner.scanBatches()) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        FieldVector rowIdVector = root.getVector(LancePayloadFetcher.ROW_ID_COLUMN);
        for (int row = 0; row < root.getRowCount(); row++) {
          rowIds.add(((Number) rowIdVector.getObject(row)).longValue());
        }
      }
    }
    return rowIds;
  }

  private static List<Integer> readIds(ArrowReader arrowReader) throws Exception {
    List<Integer> ids = new ArrayList<>();
    try (ArrowReader reader = arrowReader) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        IntVector idVector = (IntVector) root.getVector("id");
        for (int row = 0; row < root.getRowCount(); row++) {
          ids.add(idVector.get(row));
        }
      }
    }
    return ids;
  }

  private static List<String> readNames(ArrowReader arrowReader) throws Exception {
    List<String> names = new ArrayList<>();
    try (ArrowReader reader = arrowReader) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        for (int row = 0; row < root.getRowCount(); row++) {
          names.add(new String(nameVector.get(row), StandardCharsets.UTF_8));
        }
      }
    }
    return names;
  }
}
