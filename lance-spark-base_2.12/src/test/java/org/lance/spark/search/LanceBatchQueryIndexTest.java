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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The query-attribution column of a batch result, the only column a chunked batch rewrites. Tested
 * on synthetic Arrow batches so the layout rules are pinned without a dataset.
 */
class LanceBatchQueryIndexTest {
  private static final BufferAllocator ALLOCATOR = new RootAllocator();

  private static FieldVector intVector(String name, int... values) {
    IntVector vector =
        new IntVector(name, FieldType.nullable(new ArrowType.Int(32, true)), ALLOCATOR);
    vector.allocateNew(values.length);
    for (int row = 0; row < values.length; row++) {
      vector.setSafe(row, values[row]);
    }
    vector.setValueCount(values.length);
    return vector;
  }

  private static FieldVector stringVector(String name, String... values) {
    VarCharVector vector =
        new VarCharVector(name, FieldType.nullable(ArrowType.Utf8.INSTANCE), ALLOCATOR);
    vector.allocateNew(values.length);
    for (int row = 0; row < values.length; row++) {
      vector.setSafe(row, values[row].getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
    vector.setValueCount(values.length);
    return vector;
  }

  private static VectorSchemaRoot root(FieldVector... vectors) {
    Schema schema =
        new Schema(
            Arrays.stream(vectors)
                .map(FieldVector::getField)
                .collect(java.util.stream.Collectors.toList()));
    return new VectorSchemaRoot(schema, Arrays.asList(vectors), vectors[0].getValueCount());
  }

  @Test
  void theSingleAttributionColumnIsTheOneLanceCoreAppends() {
    VectorSchemaRoot root = root(intVector("query_index", 0, 1), intVector("id", 7, 8));
    try {
      assertEquals(
          "query_index",
          LanceDistributedSearchColumnarPartitionReader.queryIndexOf(root).getField().getName());
    } finally {
      root.close();
    }
  }

  /**
   * A dataset column can be called `query_index` too, and then two integer columns share the name.
   * lance-core writes its attribution column first, so that is the one a chunk base may shift.
   */
  @Test
  void anAmbiguousNameStillPrefersTheAttributionColumnLanceCoreWritesFirst() {
    VectorSchemaRoot root = root(intVector("query_index", 0, 1), intVector("query_index", 42, 43));
    try {
      IntVector vector = LanceDistributedSearchColumnarPartitionReader.queryIndexOf(root);
      assertEquals(0, vector.get(0), "the first column is the attribution column");
      assertEquals(1, vector.get(1));
    } finally {
      root.close();
    }
  }

  /** A same-named column of another type must not be shifted as if it carried query ids. */
  @Test
  void aColumnOfAnotherTypeIsNotTakenForTheAttributionColumn() {
    VectorSchemaRoot root =
        root(stringVector("query_index", "a", "b"), intVector("query_index", 0, 1));
    try {
      IntVector vector = LanceDistributedSearchColumnarPartitionReader.queryIndexOf(root);
      assertEquals(0, vector.get(0));
      assertEquals(1, vector.get(1), "the integer column is the attribution column");
    } finally {
      root.close();
    }
  }

  /** Without the column a chunked batch cannot be attributed at all, so it must not be guessed. */
  @Test
  void aBatchWithoutAnAttributionColumnFailsInsteadOfGuessing() {
    VectorSchemaRoot root = root(intVector("id", 7, 8));
    try {
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class,
              () -> LanceDistributedSearchColumnarPartitionReader.queryIndexOf(root));
      assertTrue(
          error.getMessage().contains("query_index"),
          "the error must name the missing column, got: " + error.getMessage());
    } finally {
      root.close();
    }
  }

  /** The reader shifts by the chunk base; the Arrow vector is where that happens. */
  @Test
  void shiftingABatchByTheChunkBaseKeepsTheRelativeOrder() {
    VectorSchemaRoot root =
        root(intVector("query_index", 0, 0, 1, 1), intVector("id", 7, 8, 9, 10));
    try {
      IntVector vector = LanceDistributedSearchColumnarPartitionReader.queryIndexOf(root);
      for (int row = 0; row < root.getRowCount(); row++) {
        vector.set(row, vector.get(row) + 2);
      }
      assertEquals(2, vector.get(0));
      assertEquals(2, vector.get(1));
      assertEquals(3, vector.get(3));
      assertEquals(8, ((IntVector) root.getVector("id")).get(1), "other columns stay untouched");
    } finally {
      root.close();
    }
  }
}
