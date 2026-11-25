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

import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.search.LanceSearchQuery.SearchType;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanceSearchScanBuilderTest {
  private static final StructType FULL_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "vector", DataTypes.createArrayType(DataTypes.FloatType), false, Metadata.empty()),
          });

  @Test
  void namespaceQueryRequestsOnlyThePrunedNativeColumns() {
    LanceSearchQuery query = query(false);
    LanceSearchScanBuilder builder = new LanceSearchScanBuilder(FULL_SCHEMA, query);
    assertTrue(builder instanceof SupportsPushDownRequiredColumns);

    StructType required =
        new StructType(
            new StructField[] {
              FULL_SCHEMA.apply("id"), field("_distance", DataTypes.FloatType),
            });
    ((SupportsPushDownRequiredColumns) builder).pruneColumns(required);

    LanceSearchScan scan = (LanceSearchScan) builder.build();
    assertEquals(required, scan.readSchema());
    // readSchema alone cannot see the pushdown: the schema stays right even when pruneColumns
    // stops pruning the query, so the native request is what this test has to pin.
    LanceSearchInputPartition partition = (LanceSearchInputPartition) scan.planInputPartitions()[0];
    assertEquals(Arrays.asList("id", "_distance"), partition.getQuery().getOutputColumns());
    assertEquals(
        Arrays.asList("id", "_distance"),
        partition.getQuery().toQueryTableRequest().getColumns().getColumnNames());
  }

  @Test
  void batchPruneKeepsPayloadAndOrderingColumnsButDropsConnectorInternals() {
    LanceSearchQuery query = query(true);
    StructType required =
        new StructType(
            new StructField[] {
              field("query_index", DataTypes.IntegerType),
              FULL_SCHEMA.apply("id"),
              field("_distance", DataTypes.FloatType),
              field("_rowid", DataTypes.LongType),
            });

    // query_index and _rowid are connector-internal physical columns: they never become payload
    // columns. The builder wiring that hands this pruned query to the scan is asserted end-to-end
    // in SparkDistributedVectorSearchTest, where planInputPartitions() exposes the real query.
    assertEquals(
        Arrays.asList("id", "_distance"),
        LanceSearchProjection.prune(query, required).getOutputColumns());
  }

  @Test
  void metadataOnlyProjectionKeepsTheOriginalNativeProjection() {
    LanceSearchQuery query =
        query(false).toBuilder().outputColumns(Arrays.asList("id", "vector", "_distance")).build();
    LanceSearchScanBuilder builder = new LanceSearchScanBuilder(FULL_SCHEMA, query);

    StructType rowIdOnly = new StructType(new StructField[] {field("_rowid", DataTypes.LongType)});
    ((SupportsPushDownRequiredColumns) builder).pruneColumns(rowIdOnly);

    LanceSearchScan scan = (LanceSearchScan) builder.build();
    assertEquals(rowIdOnly, scan.readSchema());
    // A projection made only of connector-internal columns must not prune to an empty native
    // column list: Lance reads that as "all columns". Fall back to what the function requested.
    LanceSearchInputPartition partition = (LanceSearchInputPartition) scan.planInputPartitions()[0];
    assertEquals(
        Arrays.asList("id", "vector", "_distance"), partition.getQuery().getOutputColumns());
    assertEquals(
        Arrays.asList("id", "vector", "_distance"),
        partition.getQuery().toQueryTableRequest().getColumns().getColumnNames());
  }

  @Test
  void vectorSearchKeepsAUserColumnNamedQueryId() {
    LanceSearchQuery query = query(false);
    StructType required =
        new StructType(
            new StructField[] {
              field("query_id", DataTypes.StringType), field("_distance", DataTypes.FloatType),
            });

    assertEquals(
        Arrays.asList("query_id", "_distance"),
        LanceSearchProjection.prune(query, required).getOutputColumns());
  }

  @Test
  void repeatedPruningWidensNestedStructsAgainstTheFullTableSchema() {
    StructType payload =
        new StructType(
            new StructField[] {
              field("a", DataTypes.IntegerType), field("b", DataTypes.IntegerType),
            });
    StructType fullSchema =
        new StructType(
            new StructField[] {field("id", DataTypes.IntegerType), field("payload", payload)});
    LanceSearchScanBuilder builder = new LanceSearchScanBuilder(fullSchema, query(false));
    SupportsPushDownRequiredColumns pushdown = builder;

    // Spark may prune more than once. The second call has to widen against the full table schema,
    // not against the first call's already rewritten one, or a nested struct pruned to its visible
    // fields would stop matching the full Arrow struct.
    pushdown.pruneColumns(new StructType(new StructField[] {field("id", DataTypes.IntegerType)}));
    pushdown.pruneColumns(
        new StructType(
            new StructField[] {
              field(
                  "payload", new StructType(new StructField[] {field("a", DataTypes.IntegerType)})),
            }));

    Scan scan = builder.build();
    assertEquals(
        payload, scan.readSchema().apply("payload").dataType(), "full struct layout restored");
  }

  private static StructField field(String name, DataType type) {
    return new StructField(name, type, true, Metadata.empty());
  }

  private static LanceSearchQuery query(boolean batch) {
    LanceSearchQuery.Builder builder =
        LanceSearchQuery.builder(SearchType.VECTOR)
            .tableId(Arrays.asList("ns", "table"))
            .namespaceImpl("dir")
            .namespaceProperties(Collections.singletonMap("root", "/tmp"))
            .readOptions(LanceSparkReadOptions.from("file:///tmp/query.lance"))
            .vectorColumn("vector")
            .topK(3);
    if (batch) {
      builder.vectors(Arrays.asList(Arrays.asList(0.0f, 0.0f), Arrays.asList(1.0f, 1.0f)));
    } else {
      builder.vector(Arrays.asList(0.0f, 0.0f));
    }
    return builder.build();
  }
}
