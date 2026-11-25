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

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

public class LanceSearchScanBuilder implements SupportsPushDownRequiredColumns {
  private StructType schema;
  private LanceSearchQuery query;

  public LanceSearchScanBuilder(StructType schema, LanceSearchQuery query) {
    this.schema = schema;
    this.query = query;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    schema = requiredSchema;
    query = LanceSearchProjection.prune(query, requiredSchema);
  }

  @Override
  public Scan build() {
    return new LanceSearchScan(schema, query);
  }
}
