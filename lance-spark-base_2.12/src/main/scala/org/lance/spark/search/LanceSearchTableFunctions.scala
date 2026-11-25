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
package org.lance.spark.search

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, CreateArray, CurrentRow, Descending, Expression, LessThanOrEqual, Literal, NamedExpression, RowFrame, RowNumber, SortOrder, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, Limit, LocalLimit, LogicalPlan, Offset, Project, Sort, Window}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{ArrayType, DataTypes, MapType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.lance.ipc.FullTextQuery
import org.lance.spark.{LanceDataset, LanceSparkReadOptions}
import org.lance.spark.search.LanceSearchQuery.SearchType
import org.lance.spark.utils.{FullTextQueryUtils, Utils}

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object LanceSearchTableFunctions {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  private val DefaultK: Integer = Integer.valueOf(10)
  private val TableArg = "table"
  private val QueryVectorArg = "query_vector"
  private val QueryArg = "query"
  private val SearchQueryArg = "search_query"
  private val ColumnsArg = "columns"
  private val SearchColumnsArg = "search_columns"
  private val DistanceMetricColumn = "_distance"
  private val FtsScoreColumn = "_score"
  private val HybridScoreColumn = "_relevance_score"
  private val RowIdColumn = "_rowid"
  private val QueryIdColumn = "query_id"

  /** The column lance-core appends to a batch result, naming the query each row belongs to. */
  private val QueryIndexColumn = "query_index"
  private val BatchRankColumn = "_batch_rank"
  private val NamedArgumentExpressionClass =
    "org.apache.spark.sql.catalyst.expressions.NamedArgumentExpression"

  def vectorSearch(args: Seq[Expression]): LogicalPlan = {
    val parsed = parseArgs("VECTOR_SEARCH", args, Seq(TableArg, QueryVectorArg, "k"))
    val tableName = requiredString(parsed, TableArg)
    // Three ways to say what to search for; resolved here so the rest of the function only sees
    // either one vector or a batch.
    val queryVector = optionalFloatArray(parsed, QueryVectorArg)
    val inlineQueries = optionalStringArray(parsed, "queries").map(parseInlineQueries)
    val queryTableName = optionalString(parsed, "query_table")
    val providedSources =
      Seq(queryVector.isDefined, inlineQueries.isDefined, queryTableName.isDefined).count(identity)
    if (providedSources > 1) {
      throw new IllegalArgumentException(
        s"VECTOR_SEARCH accepts either query_vector, queries or query_table, not several at once")
    }
    if (providedSources == 0) {
      throw new IllegalArgumentException("query_vector is required")
    }
    val batchQueries: Option[Seq[Seq[java.lang.Float]]] =
      inlineQueries.orElse(
        queryTableName.map(name =>
          readQueryTable(
            name,
            optionalString(parsed, "query_vector_column").getOrElse("vector"),
            optionalString(parsed, "query_id_column"))))
    val isBatch = batchQueries.isDefined
    val batchSize = batchQueries.map(_.size).getOrElse(0)
    val dimension = batchQueries.map(_.headOption.map(_.size).getOrElse(0)).getOrElse(0)
    val k = optionalInt(parsed, "num_results")
      .orElse(optionalInt(parsed, "limit"))
      .orElse(optionalInt(parsed, "k"))
      .getOrElse(DefaultK)
    val outputColumns = optionalStringArray(parsed, ColumnsArg).getOrElse(Seq.empty)
    val filter = optionalString(parsed, "filter").orNull
    val withRowId = effectiveWithRowId(parsed, outputColumns)
    val offset = optionalInt(parsed, "offset")
    if (isBatch && offset.isDefined) {
      throw new IllegalArgumentException(
        "offset is not supported with a batch search: every query has its own k rows")
    }
    val version = optionalLong(parsed, "version")
    val requestK = offset
      .map(value => Integer.valueOf(Math.addExact(k.intValue(), value.intValue())))
      .getOrElse(k)
    val resolved = resolveLanceTable(tableName, version)
    val schemaColumns =
      requestOutputColumns(resolved.table.schema(), outputColumns, DistanceMetricColumn)
    val requestColumns = namespaceOutputColumns(schemaColumns)
    val baseSchema =
      outputSchema(resolved.table.schema(), schemaColumns, DistanceMetricColumn, withRowId)
    val schema =
      if (isBatch) {
        StructType(
          StructField(QueryIdColumn, DataTypes.IntegerType, nullable = false) +: baseSchema.fields)
      } else {
        baseSchema
      }

    // The builder takes the Java nested list lance-core expects; built here so the element type is
    // the boxed one its signature names.
    val batchRows: java.util.List[java.util.List[java.lang.Float]] =
      batchQueries
        .map { queries =>
          val rows = new java.util.ArrayList[java.util.List[java.lang.Float]](queries.size)
          queries.foreach { query =>
            val row = new java.util.ArrayList[java.lang.Float](query.size)
            query.foreach(value => row.add(value))
            rows.add(row)
          }
          rows
        }
        .orNull

    val builder = LanceSearchQuery
      .builder(SearchType.VECTOR)
      .tableId(resolved.table.readOptions().getTableId)
      .namespaceImpl(resolved.table.getNamespaceImpl)
      .namespaceProperties(resolved.table.getNamespaceProperties)
      // Only the distributed path reads these; it opens the dataset itself.
      .readOptions(resolved.table.readOptions())
      .initialStorageOptions(resolved.table.getInitialStorageOptions)
      .outputColumns(requestColumns.asJava)
      .vector(queryVector.getOrElse(Seq.empty[java.lang.Float]).asJava)
      .vectors(batchRows)
      .topK(requestK)
      .vectorColumn(optionalString(parsed, "vector_column").orNull)
      .indexName(
        optionalString(parsed, "index").orElse(optionalString(parsed, "index_name")).orNull)
      .distanceType(optionalString(parsed, "distance_type").orNull)
      .filter(filter)
      .offset(offset.orNull)
      .version(version.orNull)
      .withRowId(withRowId.orNull)
      .nprobes(optionalInt(parsed, "nprobes").orNull)
      .ef(optionalInt(parsed, "ef").orNull)
      .refineFactor(optionalInt(parsed, "refine_factor").orNull)
      .lowerBound(optionalFloat(parsed, "lower_bound").orNull)
      .upperBound(optionalFloat(parsed, "upper_bound").orNull)
      .bypassVectorIndex(optionalBoolean(parsed, "bypass_vector_index").orNull)
      .fastSearch(optionalBoolean(parsed, "fast_search").orNull)
      .prefilter(optionalBoolean(parsed, "prefilter").orNull)

    relation("VECTOR_SEARCH", schema, builder.build(), resolved)
  }

  def search(args: Seq[Expression]): LogicalPlan = {
    val parsed = parseArgs("SEARCH", args, Seq(TableArg, QueryArg, "k"))
    val tableName = requiredString(parsed, TableArg)
    val queryText = optionalString(parsed, QueryArg)
      .orElse(optionalString(parsed, SearchQueryArg))
      .getOrElse(throw new IllegalArgumentException("SEARCH requires query"))
    val searchColumns = optionalStringArray(parsed, SearchColumnsArg).getOrElse(Seq.empty)
    if (searchColumns.isEmpty) {
      throw new IllegalArgumentException(
        "SEARCH requires search_columns for full-text search, " +
          "e.g. search_columns => array('body')")
    }
    val k = optionalInt(parsed, "num_results")
      .orElse(optionalInt(parsed, "limit"))
      .orElse(optionalInt(parsed, "k"))
      .getOrElse(DefaultK)
    val outputColumns = optionalStringArray(parsed, ColumnsArg).getOrElse(Seq.empty)
    val withRowId = effectiveWithRowId(parsed, outputColumns)
    val version = optionalLong(parsed, "version")
    val resolved = resolveLanceTable(tableName, version)

    // Build the canonical FullTextQuery and carry it through the shared scan path via options,
    // instead of the bespoke LanceSearchTable. One column -> match, several -> multi_match.
    val fullTextQuery =
      if (searchColumns.size == 1) {
        FullTextQuery.`match`(queryText, searchColumns.head)
      } else {
        FullTextQuery.multiMatch(queryText, searchColumns.asJava)
      }
    val options = new java.util.HashMap[String, String]()
    options.put(
      LanceSparkReadOptions.CONFIG_FULL_TEXT_QUERY,
      FullTextQueryUtils.fullTextQueryToString(fullTextQuery))
    version.foreach(v => options.put(LanceSparkReadOptions.CONFIG_VERSION, v.toString))

    val relation = DataSourceV2Relation.create(
      resolved.table,
      Some(resolved.catalog),
      Some(resolved.identifier),
      new CaseInsensitiveStringMap(options))

    // SEARCH is ranked top-k: optional filter, project requested columns + _score, order by
    // _score descending, limit k. k flows to the scan through Spark's limit pushdown; _score is
    // resolved as a metadata column on LanceDataset.
    val filtered = optionalString(parsed, "filter")
      .map(f =>
        Filter(SparkSession.active.sessionState.sqlParser.parseExpression(f), relation))
      .getOrElse(relation)
    val projected = Project(searchProjectList(outputColumns, withRowId), filtered)
    val ordered =
      Sort(
        Seq(SortOrder(UnresolvedAttribute(FtsScoreColumn), Descending)),
        global = true,
        projected)
    Limit(Literal(k.intValue()), ordered)
  }

  private def searchProjectList(
      outputColumns: Seq[String],
      withRowId: Option[java.lang.Boolean]): Seq[NamedExpression] = {
    val normalized = normalizeOutputColumns(outputColumns)
    val buf = ArrayBuffer.empty[NamedExpression]
    if (normalized.isEmpty) {
      buf += UnresolvedStar(None)
    } else {
      normalized.foreach(column => buf += UnresolvedAttribute.quoted(column))
    }
    if (!normalized.exists(_.equalsIgnoreCase(FtsScoreColumn))) {
      buf += UnresolvedAttribute.quoted(FtsScoreColumn)
    }
    if (withRowId.contains(java.lang.Boolean.TRUE) &&
      !normalized.exists(_.equalsIgnoreCase(RowIdColumn))) {
      buf += UnresolvedAttribute.quoted(RowIdColumn)
    }
    buf.toSeq
  }

  def hybridSearch(args: Seq[Expression]): LogicalPlan = {
    val parsed = parseArgs("HYBRID_SEARCH", args, Seq(TableArg, QueryVectorArg, QueryArg, "k"))
    val tableName = requiredString(parsed, TableArg)
    val queryVector = requiredFloatArray(parsed, QueryVectorArg)
    val queryText = optionalString(parsed, QueryArg)
      .orElse(optionalString(parsed, SearchQueryArg))
      .getOrElse(throw new IllegalArgumentException("HYBRID_SEARCH requires query"))
    val k = optionalInt(parsed, "num_results")
      .orElse(optionalInt(parsed, "limit"))
      .orElse(optionalInt(parsed, "k"))
      .getOrElse(DefaultK)
    val offset = optionalInt(parsed, "offset").getOrElse(Integer.valueOf(0))
    val candidateK = optionalInt(parsed, "candidates")
      .orElse(optionalInt(parsed, "num_candidates"))
      .orElse(optionalInt(parsed, "candidate_count"))
      .map(value => Integer.valueOf(math.max(value.intValue(), k.intValue() + offset.intValue())))
      .getOrElse(Integer.valueOf(k.intValue() + offset.intValue()))
    val outputColumns = optionalStringArray(parsed, ColumnsArg).getOrElse(Seq.empty)
    val filter = optionalString(parsed, "filter").orNull
    val withRowId = effectiveWithRowId(parsed, outputColumns)
    val rrfK = optionalFloat(parsed, "rrf_k").getOrElse(java.lang.Float.valueOf(60.0f))
    val version = optionalLong(parsed, "version")

    val resolved = resolveLanceTable(tableName, version)
    Utils.validateFullTextSearchSchema(resolved.table.schema())
    val vectorSchemaColumns =
      hybridSideOutputColumns(resolved.table.schema(), outputColumns, DistanceMetricColumn)
    val ftsSchemaColumns =
      hybridSideOutputColumns(resolved.table.schema(), outputColumns, FtsScoreColumn)
    val vectorSchema =
      outputSchema(
        resolved.table.schema(),
        vectorSchemaColumns,
        DistanceMetricColumn,
        Some(java.lang.Boolean.TRUE))
    val ftsSchema =
      outputSchema(
        resolved.table.schema(),
        ftsSchemaColumns,
        FtsScoreColumn,
        Some(java.lang.Boolean.TRUE))
    val schema = hybridOutputSchema(resolved.table.schema(), outputColumns, withRowId)

    val vectorRequestColumns = namespaceOutputColumns(vectorSchemaColumns)
    val ftsRequestColumns = namespaceOutputColumns(ftsSchemaColumns)

    val vectorQuery = LanceSearchQuery
      .builder(SearchType.VECTOR)
      .tableId(resolved.table.readOptions().getTableId)
      .namespaceImpl(resolved.table.getNamespaceImpl)
      .namespaceProperties(resolved.table.getNamespaceProperties)
      .outputColumns(vectorRequestColumns.asJava)
      .vector(queryVector.asJava)
      .topK(candidateK)
      .vectorColumn(optionalString(parsed, "vector_column").orNull)
      .distanceType(optionalString(parsed, "distance_type").orNull)
      .filter(filter)
      .version(version.orNull)
      .withRowId(java.lang.Boolean.TRUE)
      .nprobes(optionalInt(parsed, "nprobes").orNull)
      .ef(optionalInt(parsed, "ef").orNull)
      .refineFactor(optionalInt(parsed, "refine_factor").orNull)
      .lowerBound(optionalFloat(parsed, "lower_bound").orNull)
      .upperBound(optionalFloat(parsed, "upper_bound").orNull)
      .bypassVectorIndex(optionalBoolean(parsed, "bypass_vector_index").orNull)
      .fastSearch(optionalBoolean(parsed, "fast_search").orNull)
      .prefilter(optionalBoolean(parsed, "prefilter").orNull)
      .build()

    val ftsQuery = LanceSearchQuery
      .builder(SearchType.FULL_TEXT)
      .tableId(resolved.table.readOptions().getTableId)
      .namespaceImpl(resolved.table.getNamespaceImpl)
      .namespaceProperties(resolved.table.getNamespaceProperties)
      .outputColumns(ftsRequestColumns.asJava)
      .textQuery(queryText)
      .searchColumns(optionalStringArray(parsed, SearchColumnsArg).getOrElse(Seq.empty).asJava)
      .topK(candidateK)
      .filter(filter)
      .prefilter(optionalBoolean(parsed, "prefilter").orNull)
      .version(version.orNull)
      .withRowId(java.lang.Boolean.TRUE)
      .build()

    val hybridQuery =
      new LanceHybridSearchQuery(vectorQuery, ftsQuery, vectorSchema, ftsSchema, k, offset, rrfK)
    hybridRelation("HYBRID_SEARCH", schema, hybridQuery, resolved)
  }

  private def relation(
      functionName: String,
      schema: StructType,
      query: LanceSearchQuery,
      resolved: ResolvedLanceTable): LogicalPlan = {
    if (shouldUseDistributed(query)) {
      distributedRelation(functionName, schema, query, resolved)
    } else {
      if (query.isBatch) {
        throw new IllegalArgumentException(
          s"$functionName with a batch of queries requires distributed search: the namespace " +
            "queryTable response carries no query attribution, so every row would be missing its " +
            "query_id. Set spark.sql.lance.search.distributed.enabled=true to run it as Spark " +
            "tasks.")
      }
      val requestedIndex = query.getIndexName
      if (requestedIndex != null && !requestedIndex.isEmpty) {
        // The namespace queryTable request has no index field, so the server would pick the index
        // itself. Failing here is the only honest answer: silently ignoring the name is exactly
        // the "measured the wrong index" failure this option exists to prevent.
        throw new IllegalArgumentException(
          s"$functionName index => '$requestedIndex' requires distributed search: the namespace " +
            "queryTable API cannot choose an index, so the server would pick it. Set " +
            "spark.sql.lance.search.distributed.enabled=true to search a named index.")
      }
      searchRelation(functionName, schema, query, resolved)
    }
  }

  /** Search served by the namespace itself: one partition, results already globally ranked. */
  private def searchRelation(
      functionName: String,
      schema: StructType,
      query: LanceSearchQuery,
      resolved: ResolvedLanceTable): LogicalPlan =
    DataSourceV2Relation.create(
      new LanceSearchTable(functionName, schema, query),
      Some(resolved.catalog),
      Some(resolved.identifier),
      CaseInsensitiveStringMap.empty())

  /**
   * Search executed by Spark tasks, one per searchable unit of the dataset. Every worker requests
   * `k + offset` rows (LanceSearchQuery.k = userK + offset, set by
   * LanceSearchTableFunctions.vectorSearch), so the merge has to happen here: sort globally, drop
   * the first `offset` rows, then take `userK`.
   */
  private def distributedRelation(
      functionName: String,
      schema: StructType,
      query: LanceSearchQuery,
      resolved: ResolvedLanceTable): LogicalPlan = {
    // lance-core sorts by (`_distance`, row id); on distance alone, tied rows land on the k and
    // offset boundaries in task arrival order and paging stops being repeatable. So read the row
    // id even when the query did not ask for it, and project it away afterwards.
    val wantsRowId = java.lang.Boolean.TRUE == query.getWithRowId
    // A batch carries its query allocation through the native `query_index` column; `query_id` is
    // produced on top of the scan, so it must not be part of the scan's own schema.
    val relationSchema =
      if (query.isBatch) StructType(schema.fields.filterNot(_.name == QueryIdColumn))
      else schema
    val scanSchema =
      if (wantsRowId) relationSchema
      else relationSchema.add(StructField(RowIdColumn, DataTypes.LongType, nullable = true))
    val scanQuery = if (wantsRowId) query else query.toBuilder.withRowId(true).build()

    val batchSchema =
      if (scanQuery.isBatch) {
        // lance-core appends the query each row belongs to; the reader needs the column in the
        // schema for it to survive into the Spark batch.
        StructType(
          StructField(
            QueryIndexColumn,
            DataTypes.IntegerType,
            nullable = false) +: scanSchema.fields)
      } else {
        scanSchema
      }

    // Everything the ranking does not read is payload: it is either read inside the native search
    // (fused) or fetched by row id after the global ranking (deferred).
    val payloadFields =
      batchSchema.fields.filterNot(field => isRankingColumn(field.name, scanQuery.isBatch)).toSeq
    val candidates =
      scanQuery.getK.longValue() * math.max(scanQuery.getQueryCount().toLong, 1L)
    // The internal ranking columns the child carries, in the order the ranking plan exposes them.
    val narrowColumns =
      if (scanQuery.isBatch) {
        Seq(QueryIndexColumn, DistanceMetricColumn, RowIdColumn)
      } else {
        Seq(DistanceMetricColumn, RowIdColumn)
      }
    // A payload column cannot be deferred when takeRows would not round-trip it (nested layout) or
    // when its name collides with a narrow column, which would make the two indistinguishable.
    val unsupportedPayload =
      payloadFields.filter { field =>
        !isDeferrablePayload(field) ||
        narrowColumns.exists(_.equalsIgnoreCase(field.name))
      }.map(_.name)
    val deferral =
      LancePayloadDeferral.plan(
        SparkSession.active,
        batchSchema,
        payloadFields.map(_.name),
        candidates,
        unsupportedPayload)

    // The scan pins the snapshot while it plans; the fetch node reads that pin afterwards so the
    // payload comes from the version the search actually saw.
    val versionPin = new LanceSearchVersionPin()

    def relation(tableSchema: StructType): LogicalPlan =
      DataSourceV2Relation.create(
        new LanceDistributedSearchTable(functionName, tableSchema, scanQuery, versionPin),
        Some(resolved.catalog),
        Some(resolved.identifier),
        CaseInsensitiveStringMap.empty())

    if (logger.isInfoEnabled) {
      val columns = payloadFields.map(_.name).mkString("[", ", ", "]")
      deferral match {
        case Some(settings) =>
          logger.info(
            s"VECTOR_SEARCH deferred payload: columns=$columns, candidatesPerUnit=$candidates, " +
              s"estimatedRowWidth=${settings.estimatedRowWidth}, " +
              s"estimatedBytesPerUnit=${settings.estimatedPayloadBytes}, " +
              s"chunkRows=${settings.chunkRows}")
        case None =>
          logger.info(
            s"VECTOR_SEARCH fused payload: columns=$columns, candidatesPerUnit=$candidates")
      }
    }

    val ranked = deferral match {
      case None =>
        rankSearch(relation(batchSchema), scanQuery)
      case Some(settings) =>
        val narrowSchema =
          StructType(
            batchSchema.fields.filterNot(field => payloadFields.exists(_.name == field.name)))
        val winners = rankSearch(relation(narrowSchema), scanQuery)
        fetchPayload(
          winners,
          batchSchema,
          payloadFields.map(_.name),
          narrowColumns,
          settings,
          scanQuery,
          resolved,
          versionPin)
    }

    finishSearch(ranked, scanQuery.isBatch, schema, wantsRowId)
  }

  /** Whether a column is one the ranking itself needs, and so stays in the narrow scan. */
  private def isRankingColumn(name: String, batch: Boolean): Boolean =
    name.equalsIgnoreCase(DistanceMetricColumn) ||
      name.equalsIgnoreCase(RowIdColumn) ||
      (batch && name.equalsIgnoreCase(QueryIndexColumn))

  /** Whether a payload column can be fetched by row id without changing its Arrow layout. */
  private def isDeferrablePayload(field: StructField): Boolean = field.dataType match {
    case _: StructType | _: MapType => false
    case ArrayType(element, _) =>
      !element.isInstanceOf[StructType] && !element.isInstanceOf[MapType]
    case _ => true
  }

  private def columnOf(plan: LogicalPlan, name: String): Attribute =
    plan.output
      .find(attr => attr.name == name)
      .getOrElse {
        throw new IllegalStateException(
          s"Internal column ${name} is missing from vector_search plan.")
      }

  /** The global ranking of a search: sort and limit one query, rank inside each query for a batch. */
  private def rankSearch(rel: LogicalPlan, query: LanceSearchQuery): LogicalPlan = {
    if (query.isBatch) {
      // A batch cannot reuse the sort-and-limit below: that keeps the global k, and the rows a
      // query needs can be any distance, so ranking has to happen inside each query first.
      return batchRanking(rel, query.getK)
    }

    val sorted =
      Sort(
        Seq(
          SortOrder(columnOf(rel, DistanceMetricColumn), Ascending),
          SortOrder(columnOf(rel, RowIdColumn), Ascending)),
        global = true,
        rel)
    val totalCandidates = query.getK
    val effectiveOffset =
      if (query.getOffset != null) query.getOffset.intValue() else 0
    val userK = math.max(totalCandidates - effectiveOffset, 0)
    val candidateLimited =
      GlobalLimit(Literal(totalCandidates), LocalLimit(Literal(totalCandidates), sorted))
    if (effectiveOffset > 0) {
      GlobalLimit(
        Literal(userK),
        LocalLimit(Literal(userK), Offset(Literal(effectiveOffset), candidateLimited)))
    } else {
      candidateLimited
    }
  }

  /** Adds the payload columns of the declared schema to the narrow result by row id. */
  private def fetchPayload(
      winners: LogicalPlan,
      outputSchema: StructType,
      payloadNames: Seq[String],
      narrowColumns: Seq[String],
      settings: LancePayloadDeferral.Settings,
      query: LanceSearchQuery,
      resolved: ResolvedLanceTable,
      versionPin: LanceSearchVersionPin): LogicalPlan = {
    // The payload columns have to be attributes of the child, not invented ones: Spark rejects a
    // plan whose output names attributes its input does not provide. They are null placeholders
    // here and this node replaces their values with the fetched payload; the scan below never reads
    // them, because the placeholder references no input column.
    val childProjection: Seq[NamedExpression] = outputSchema.fields.map { field =>
      if (payloadNames.contains(field.name)) {
        Alias(
          LancePayloadPlaceholder(field.name, field.dataType, field.nullable),
          field.name)(): NamedExpression
      } else {
        columnOf(winners, field.name)
      }
    }.toSeq
    val child = Project(childProjection, winners)
    // The placeholder carries the declared data type and nullability, so the node's output can
    // simply mirror the child: a NOT NULL payload column stays NOT NULL.
    val columns = child.output
    val fetched = columns.filter(column => payloadNames.contains(column.name))
    LanceFetchPayload(
      child,
      columns,
      fetched,
      narrowColumns,
      query.isBatch,
      new LanceFetchPayloadSource(
        query.getReadOptions,
        resolved.table.getInitialStorageOptions,
        query.getNamespaceImpl,
        query.getNamespaceProperties),
      settings.chunkRows,
      versionPin)
  }

  /** The caller-visible projection: `query_id` for a batch, `_rowid` only when it was asked for. */
  private def finishSearch(
      plan: LogicalPlan,
      isBatch: Boolean,
      schema: StructType,
      wantsRowId: Boolean): LogicalPlan = {
    if (isBatch) {
      val projected = schema.fields.map { field =>
        val source =
          if (field.name == QueryIdColumn) columnOf(plan, QueryIndexColumn)
          else columnOf(plan, field.name)
        Alias(source, field.name)(): NamedExpression
      }.toSeq
      Project(projected, plan)
    } else if (wantsRowId) {
      plan
    } else {
      Project(plan.output.filterNot(attr => attr.name == RowIdColumn), plan)
    }
  }

  /**
   * The global top-k of every query: each task returns its own top k per query, and the merge has
   * to keep k per query rather than k overall, so the rows are ranked inside their query first.
   * Row id breaks ties the way lance-core does (by `_distance`, then row id).
   */
  private def batchRanking(rel: LogicalPlan, k: Integer): LogicalPlan = {
    val partitionSpec = Seq(columnOf(rel, QueryIndexColumn))
    val orderSpec =
      Seq(
        SortOrder(columnOf(rel, DistanceMetricColumn), Ascending),
        SortOrder(columnOf(rel, RowIdColumn), Ascending))
    // A bare RowNumber inside Window is rejected by the analyzer; the window function has to carry
    // its OVER clause the way the parser builds it.
    val ranking =
      Window(
        Seq(
          Alias(
            WindowExpression(
              RowNumber(),
              WindowSpecDefinition(
                partitionSpec,
                orderSpec,
                SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))),
            BatchRankColumn)()),
        partitionSpec,
        orderSpec,
        rel)

    val rankColumn = columnOf(ranking, BatchRankColumn)
    Filter(LessThanOrEqual(rankColumn, Literal(k)), ranking)
  }

  private def shouldUseDistributed(query: LanceSearchQuery): Boolean = {
    val spark = SparkSession.active
    // Not String.toBoolean: it throws on anything but "true"/"false", and this runs for every
    // VECTOR_SEARCH.
    val enabled = java.lang.Boolean.parseBoolean(
      spark.conf.get("spark.sql.lance.search.distributed.enabled", "false"))
    if (!enabled) return false
    // A distributed plan can only prefilter, while namespace execution defaults to post-filtering
    // the top k, which returns a different set of rows. Make the user pick.
    val hasFilter = query.getFilter != null && !query.getFilter.isEmpty
    if (hasFilter && java.lang.Boolean.TRUE != query.getPrefilter) {
      throw new IllegalArgumentException(
        "Distributed VECTOR_SEARCH with a filter requires prefilter=true, which returns the true " +
          "filtered top k; disable spark.sql.lance.search.distributed.enabled to keep namespace " +
          "post-filter semantics, where the filter is applied after the top k is chosen")
    }
    if (java.lang.Boolean.FALSE == query.getPrefilter) {
      throw new IllegalArgumentException(
        "Distributed VECTOR_SEARCH does not support prefilter=false; disable " +
          "spark.sql.lance.search.distributed.enabled to use namespace execution")
    }
    // lance-core's Java scanner cannot apply distance bounds yet.
    if (query.getLowerBound != null || query.getUpperBound != null) {
      throw new IllegalArgumentException(
        "Distributed VECTOR_SEARCH does not support lower_bound or upper_bound; disable " +
          "spark.sql.lance.search.distributed.enabled to use namespace execution")
    }
    true
  }

  private def hybridRelation(
      functionName: String,
      schema: StructType,
      query: LanceHybridSearchQuery,
      resolved: ResolvedLanceTable): LogicalPlan = {
    val table = new LanceHybridSearchTable(functionName, schema, query)
    DataSourceV2Relation.create(
      table,
      Some(resolved.catalog),
      Some(resolved.identifier),
      CaseInsensitiveStringMap.empty())
  }

  /** Parses the inline `queries => array('0.1,0.2', ...)` form, one comma-separated list each. */
  private def parseInlineQueries(encoded: Seq[String]): Seq[Seq[java.lang.Float]] = {
    if (encoded.isEmpty) {
      throw new IllegalArgumentException("queries must name at least one query vector")
    }
    val parsed = encoded.map { one =>
      val values = one.split(",").map(_.trim)
      if (values.exists(_.isEmpty)) {
        throw new IllegalArgumentException(
          s"Every query in queries must be a comma-separated list of numbers, got: '$one'")
      }
      values.map { value =>
        try {
          java.lang.Float.valueOf(value)
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"'$value' in queries is not a number (query: '$one')")
        }
      }.toSeq
    }
    val dimension = parsed.head.size
    parsed.zipWithIndex.foreach { case (query, index) =>
      if (query.size != dimension) {
        throw new IllegalArgumentException(
          s"Every query vector must have dimension $dimension, but query $index has ${query.size}")
      }
    }
    parsed
  }

  /**
   * Reads one query vector per row of a table, ordered by its id column when one is given so the
   * `query_id` a result carries means the same thing on every run.
   */
  private def readQueryTable(
      name: String,
      vectorColumn: String,
      idColumn: Option[String]): Seq[Seq[java.lang.Float]] = {
    val spark = SparkSession.active
    val qualified = name
    val sql =
      s"SELECT `$vectorColumn` FROM $qualified" + idColumn.map(id => s" ORDER BY `$id`").getOrElse(
        "")
    val queries = scala.collection.mutable.ArrayBuffer.empty[Seq[java.lang.Float]]
    val iterator = spark.sql(sql).toLocalIterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val values = row.getList[Any](0)
      if (values != null) {
        val query = new Array[java.lang.Float](values.size())
        var i = 0
        while (i < query.length) {
          query(i) = java.lang.Float.valueOf(values.get(i).asInstanceOf[Number].floatValue())
          i += 1
        }
        queries += query.toSeq
      }
    }
    if (queries.isEmpty) {
      throw new IllegalArgumentException(
        s"query_table '$name' holds no vector rows; a batch search needs at least one query")
    }
    val dimension = queries.head.size
    queries.zipWithIndex.foreach { case (query, index) =>
      if (query.size != dimension) {
        throw new IllegalArgumentException(
          s"Every query vector must have dimension $dimension, but row $index of '$name' has " +
            s"${query.size}")
      }
    }
    queries.toSeq
  }

  private def parseArgs(
      functionName: String,
      args: Seq[Expression],
      positionalNames: Seq[String]): ParsedArgs = {
    val named = scala.collection.mutable.LinkedHashMap.empty[String, Expression]
    val positional = ArrayBuffer.empty[Expression]

    args.foreach { expr =>
      namedArgument(expr) match {
        case Some((key, value)) =>
          named.put(normalizeName(key), value)
        case None =>
          positional += expr
      }
    }

    if (named.nonEmpty && positional.nonEmpty) {
      throw new IllegalArgumentException(
        s"$functionName does not support mixing named and positional arguments")
    }
    if (named.nonEmpty) {
      ParsedArgs(named.toMap)
    } else {
      if (positional.size > positionalNames.size) {
        throw new IllegalArgumentException(s"$functionName received too many positional arguments")
      }
      ParsedArgs(positionalNames.zip(positional).map { case (name, expr) => name -> expr }.toMap)
    }
  }

  private def requiredString(parsed: ParsedArgs, name: String): String =
    optionalString(parsed, name).getOrElse(throw new IllegalArgumentException(s"$name is required"))

  private def requiredFloatArray(parsed: ParsedArgs, name: String): Seq[java.lang.Float] =
    optionalFloatArray(parsed, name).getOrElse(
      throw new IllegalArgumentException(s"$name is required"))

  private def optionalString(parsed: ParsedArgs, name: String): Option[String] =
    parsed.get(name).map {
      case attr: UnresolvedAttribute => attr.name
      case expr => literalValue(expr) match {
          case value: org.apache.spark.unsafe.types.UTF8String => value.toString
          case value: String => value
          case null => null
          case other => other.toString
        }
    }

  private def optionalStringArray(parsed: ParsedArgs, name: String): Option[Seq[String]] =
    parsed.get(name).map(expr =>
      literalArray(expr).map {
        case value: org.apache.spark.unsafe.types.UTF8String => value.toString
        case value: String => value
        case other => other.toString
      })

  private def optionalFloatArray(parsed: ParsedArgs, name: String): Option[Seq[java.lang.Float]] =
    parsed.get(name).map(expr => literalArray(expr).map(toFloat))

  private def optionalInt(parsed: ParsedArgs, name: String): Option[Integer] =
    parsed.get(name).map(value => Integer.valueOf(toNumber(literalValue(value)).intValue()))

  private def optionalLong(parsed: ParsedArgs, name: String): Option[java.lang.Long] =
    parsed.get(name).map(value => java.lang.Long.valueOf(toNumber(literalValue(value)).longValue()))

  private def optionalFloat(parsed: ParsedArgs, name: String): Option[java.lang.Float] =
    parsed.get(name).map(value => toFloat(literalValue(value)))

  private def optionalBoolean(parsed: ParsedArgs, name: String): Option[java.lang.Boolean] =
    parsed.get(name).map(value =>
      java.lang.Boolean.valueOf(literalValue(value).asInstanceOf[Boolean]))

  private def literalArray(expr: Expression): Seq[Any] = expr match {
    case array: CreateArray => array.children.map(literalValue)
    case literal: Literal if literal.value == null => Seq.empty
    case other =>
      literalValue(other) match {
        case values: Seq[_] => values
        case values: Array[_] => values.toSeq
        case value =>
          throw new IllegalArgumentException(s"Expected array literal, got $value")
      }
  }

  private def literalValue(expr: Expression): Any = expr match {
    case literal: Literal => literal.value
    case array: CreateArray => array.children.map(literalValue)
    case other if other.foldable => other.eval(InternalRow.empty)
    case other =>
      throw new IllegalArgumentException(s"Argument must be a foldable literal: ${other.sql}")
  }

  private def toNumber(value: Any): Number = value match {
    case number: Number => number
    case decimal: org.apache.spark.sql.types.Decimal => decimal.toJavaBigDecimal
    case other => throw new IllegalArgumentException(s"Expected numeric literal, got $other")
  }

  private def toFloat(value: Any): java.lang.Float =
    java.lang.Float.valueOf(toNumber(value).floatValue())

  private def namedArgument(expr: Expression): Option[(String, Expression)] = {
    if (expr.getClass.getName != NamedArgumentExpressionClass) {
      None
    } else {
      val key = expr.getClass.getMethod("key").invoke(expr).asInstanceOf[String]
      val value = expr.getClass.getMethod("value").invoke(expr).asInstanceOf[Expression]
      Some((key, value))
    }
  }

  private def normalizeName(name: String): String =
    name.toLowerCase(Locale.ROOT)

  private def normalizeOutputColumns(columns: Seq[String]): Seq[String] =
    if (columns.exists(_ == "*")) {
      Seq.empty
    } else {
      columns
    }

  private def requestOutputColumns(
      baseSchema: StructType,
      columns: Seq[String],
      metricName: String): Seq[String] = {
    val normalizedColumns = normalizeOutputColumns(columns)
    if (normalizedColumns.isEmpty) {
      normalizedColumns
    } else {
      val resolvedColumns = normalizedColumns.map { column =>
        if (column.equalsIgnoreCase(metricName)) {
          metricName
        } else if (column.equalsIgnoreCase(RowIdColumn)) {
          RowIdColumn
        } else {
          findField(baseSchema, column).name
        }
      }
      if (resolvedColumns.exists(_.equalsIgnoreCase(metricName))) {
        resolvedColumns
      } else {
        resolvedColumns :+ metricName
      }
    }
  }

  private def hybridSideOutputColumns(
      baseSchema: StructType,
      columns: Seq[String],
      metricName: String): Seq[String] = {
    val normalizedColumns = normalizeOutputColumns(columns)
    if (normalizedColumns.isEmpty) {
      normalizedColumns
    } else {
      val resolvedColumns = normalizedColumns.flatMap { column =>
        if (isHybridMetricColumn(column) || column.equalsIgnoreCase(RowIdColumn)) {
          None
        } else {
          Some(findField(baseSchema, column).name)
        }
      }
      if (resolvedColumns.exists(_.equalsIgnoreCase(metricName))) {
        resolvedColumns
      } else {
        resolvedColumns :+ metricName
      }
    }
  }

  private def namespaceOutputColumns(columns: Seq[String]): Seq[String] =
    normalizeOutputColumns(columns).filterNot(_.equalsIgnoreCase(RowIdColumn))

  private def effectiveWithRowId(
      parsed: ParsedArgs,
      outputColumns: Seq[String]): Option[java.lang.Boolean] =
    if (normalizeOutputColumns(outputColumns).exists(_.equalsIgnoreCase(RowIdColumn))) {
      Some(java.lang.Boolean.TRUE)
    } else {
      optionalBoolean(parsed, "with_row_id")
    }

  private def outputSchema(
      baseSchema: StructType,
      columns: Seq[String],
      metricName: String,
      withRowId: Option[java.lang.Boolean]): StructType = {
    val normalizedColumns = normalizeOutputColumns(columns)
    val fields =
      if (normalizedColumns.isEmpty) {
        baseSchema.fields.toSeq
      } else {
        normalizedColumns.map { column =>
          if (column.equalsIgnoreCase(metricName)) {
            StructField(metricName, DataTypes.FloatType, nullable = true)
          } else if (column.equalsIgnoreCase(RowIdColumn)) {
            StructField(RowIdColumn, DataTypes.LongType, nullable = true)
          } else {
            findField(baseSchema, column)
          }
        }
      }
    val result = ArrayBuffer(fields: _*)
    if (!result.exists(_.name.equalsIgnoreCase(metricName))) {
      result += StructField(metricName, DataTypes.FloatType, nullable = true)
    }
    if (withRowId.contains(java.lang.Boolean.TRUE) &&
      !result.exists(_.name.equalsIgnoreCase(RowIdColumn))) {
      result += StructField(RowIdColumn, DataTypes.LongType, nullable = true)
    }
    new StructType(result.toArray)
  }

  private def hybridOutputSchema(
      baseSchema: StructType,
      columns: Seq[String],
      withRowId: Option[java.lang.Boolean]): StructType = {
    val normalizedColumns = normalizeOutputColumns(columns)
    val fields =
      if (normalizedColumns.isEmpty) {
        baseSchema.fields.toSeq
      } else {
        normalizedColumns.map { column =>
          if (column.equalsIgnoreCase(DistanceMetricColumn)) {
            StructField(DistanceMetricColumn, DataTypes.FloatType, nullable = true)
          } else if (column.equalsIgnoreCase(FtsScoreColumn)) {
            StructField(FtsScoreColumn, DataTypes.FloatType, nullable = true)
          } else if (column.equalsIgnoreCase(HybridScoreColumn)) {
            StructField(HybridScoreColumn, DataTypes.FloatType, nullable = false)
          } else if (column.equalsIgnoreCase(RowIdColumn)) {
            StructField(RowIdColumn, DataTypes.LongType, nullable = true)
          } else {
            findField(baseSchema, column)
          }
        }
      }
    val result = ArrayBuffer(fields: _*)
    if (!result.exists(_.name.equalsIgnoreCase(DistanceMetricColumn))) {
      result += StructField(DistanceMetricColumn, DataTypes.FloatType, nullable = true)
    }
    if (!result.exists(_.name.equalsIgnoreCase(FtsScoreColumn))) {
      result += StructField(FtsScoreColumn, DataTypes.FloatType, nullable = true)
    }
    if (!result.exists(_.name.equalsIgnoreCase(HybridScoreColumn))) {
      result += StructField(HybridScoreColumn, DataTypes.FloatType, nullable = false)
    }
    if (withRowId.contains(java.lang.Boolean.TRUE) &&
      !result.exists(_.name.equalsIgnoreCase(RowIdColumn))) {
      result += StructField(RowIdColumn, DataTypes.LongType, nullable = true)
    }
    new StructType(result.toArray)
  }

  private def isHybridMetricColumn(column: String): Boolean =
    column.equalsIgnoreCase(DistanceMetricColumn) ||
      column.equalsIgnoreCase(FtsScoreColumn) ||
      column.equalsIgnoreCase(HybridScoreColumn)

  private def findField(schema: StructType, column: String): StructField =
    schema.fields
      .find(_.name.equalsIgnoreCase(column))
      .getOrElse(throw new IllegalArgumentException(s"Unknown column '$column'"))

  private def resolveLanceTable(
      tableName: String,
      version: Option[java.lang.Long]): ResolvedLanceTable = {
    val spark = SparkSession.active
    val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName).toArray
    if (parts.isEmpty) {
      throw new IllegalArgumentException("table is required")
    }
    val catalogManager = spark.sessionState.catalogManager
    val (catalog, identifier) =
      if (parts.length >= 2 && isConfiguredCatalog(spark, parts.head)) {
        val catalog = catalogManager.catalog(parts.head)
        val identParts = parts.tail
        (catalog, Identifier.of(identParts.dropRight(1), identParts.last))
      } else {
        val namespace =
          if (parts.length > 1) parts.dropRight(1) else catalogManager.currentNamespace
        (catalogManager.currentCatalog, Identifier.of(namespace, parts.last))
      }

    val tableCatalog = catalog match {
      case tableCatalog: TableCatalog => tableCatalog
      case other =>
        throw new IllegalArgumentException(s"Catalog '${other.name()}' is not a table catalog")
    }

    val table =
      version
        .map(value => tableCatalog.loadTable(identifier, value.toString))
        .getOrElse(tableCatalog.loadTable(identifier))
    table match {
      case table: LanceDataset =>
        if (table.getNamespaceImpl == null || table.readOptions().getTableId == null) {
          throw new IllegalArgumentException(
            "Lance search table functions require namespace-backed Lance tables")
        }
        ResolvedLanceTable(tableCatalog, identifier, table)
      case other =>
        throw new IllegalArgumentException(
          s"Table '$tableName' is not a Lance table: ${other.getClass.getName}")
    }
  }

  private def isConfiguredCatalog(spark: SparkSession, name: String): Boolean =
    spark.sessionState.catalogManager.isCatalogRegistered(name) ||
      spark.conf.getOption(s"spark.sql.catalog.$name").isDefined

  private case class ParsedArgs(args: Map[String, Expression]) {
    def get(name: String): Option[Expression] = args.get(normalizeName(name))
  }

  private case class ResolvedLanceTable(
      catalog: TableCatalog,
      identifier: Identifier,
      table: LanceDataset)
}
