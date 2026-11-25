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
import org.apache.spark.sql.types.DataTypes;
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
            new StructField(
                "id", DataTypes.IntegerType, false, org.apache.spark.sql.types.Metadata.empty()),
            new StructField(
                "vector",
                DataTypes.createArrayType(DataTypes.FloatType),
                false,
                org.apache.spark.sql.types.Metadata.empty()),
          });

  @Test
  void namespaceSearchPrunesTheScanSchemaToRequiredColumns() {
    LanceSearchQuery query = query(false);
    LanceSearchScanBuilder builder = new LanceSearchScanBuilder(FULL_SCHEMA, query);
    assertTrue(builder instanceof SupportsPushDownRequiredColumns);

    StructType required =
        new StructType(
            new StructField[] {
              FULL_SCHEMA.apply("id"),
              new StructField(
                  "_distance",
                  DataTypes.FloatType,
                  true,
                  org.apache.spark.sql.types.Metadata.empty()),
            });
    ((SupportsPushDownRequiredColumns) builder).pruneColumns(required);

    Scan scan = builder.build();
    assertEquals(required, scan.readSchema());
    assertEquals(
        Arrays.asList("id", "_distance"),
        LanceSearchProjection.prune(query, required).getOutputColumns());
  }

  @Test
  void distributedBatchPrunesPayloadButKeepsQueryAttributionAndOrderingColumns() {
    LanceSearchQuery query = query(true);
    LanceDistributedSearchScanBuilder builder =
        new LanceDistributedSearchScanBuilder(FULL_SCHEMA, query);
    assertTrue(builder instanceof SupportsPushDownRequiredColumns);

    StructType required =
        new StructType(
            new StructField[] {
              new StructField(
                  "query_index",
                  DataTypes.IntegerType,
                  false,
                  org.apache.spark.sql.types.Metadata.empty()),
              FULL_SCHEMA.apply("id"),
              new StructField(
                  "_distance",
                  DataTypes.FloatType,
                  true,
                  org.apache.spark.sql.types.Metadata.empty()),
              new StructField(
                  "_rowid", DataTypes.LongType, true, org.apache.spark.sql.types.Metadata.empty()),
            });
    ((SupportsPushDownRequiredColumns) builder).pruneColumns(required);

    Scan scan = builder.build();
    assertEquals(required, scan.readSchema());
    assertEquals(
        Arrays.asList("id", "_distance"),
        LanceSearchProjection.prune(query, required).getOutputColumns());
  }

  @Test
  void metadataOnlyProjectionKeepsTheOriginalNativeProjection() {
    LanceSearchQuery query = query(false);
    StructType required =
        new StructType(
            new StructField[] {
              new StructField(
                  "_distance",
                  DataTypes.FloatType,
                  true,
                  org.apache.spark.sql.types.Metadata.empty()),
              new StructField(
                  "_rowid", DataTypes.LongType, true, org.apache.spark.sql.types.Metadata.empty()),
            });

    assertEquals(
        Collections.singletonList("_distance"),
        LanceSearchProjection.prune(query, required).getOutputColumns());
  }

  @Test
  void vectorSearchKeepsAUserColumnNamedQueryId() {
    LanceSearchQuery query = query(false);
    StructType required =
        new StructType(
            new StructField[] {
              new StructField(
                  "query_id",
                  DataTypes.StringType,
                  true,
                  org.apache.spark.sql.types.Metadata.empty()),
              new StructField(
                  "_distance",
                  DataTypes.FloatType,
                  true,
                  org.apache.spark.sql.types.Metadata.empty()),
            });

    assertEquals(
        Arrays.asList("query_id", "_distance"),
        LanceSearchProjection.prune(query, required).getOutputColumns());
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
