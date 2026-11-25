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

import org.lance.spark.LanceConstant;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/** Keeps the internal columns required by a search plan while pruning payload columns. */
final class LanceSearchProjection {
  private LanceSearchProjection() {}

  static LanceSearchQuery prune(LanceSearchQuery query, StructType requiredSchema) {
    List<String> requested = new ArrayList<>();
    for (StructField field : requiredSchema.fields()) {
      String name = field.name();
      if (isInternal(query, name)) {
        continue;
      }
      requested.add(name);
    }

    // A required schema made only of metadata/order columns (for example COUNT(*) or a row-id-only
    // projection) must not turn into an empty native column list: an empty Lance projection means
    // "all columns". Keep the original request in that case; the scan schema still governs Spark's
    // visible output.
    if (requested.isEmpty()) {
      return query;
    }
    return query.toBuilder().outputColumns(requested).build();
  }

  private static boolean isInternal(LanceSearchQuery query, String name) {
    // `_distance` and `_score` are generated result columns and remain in the native request. Only
    // row id and a batch's query_index are connector-internal physical columns.
    return name.equalsIgnoreCase(LanceConstant.ROW_ID)
        || (query.isBatch() && name.equalsIgnoreCase("query_index"));
  }
}
