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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.SparkContext
import org.apache.spark.sql.util.LanceSerializeUtil.{decode, encode}
import org.lance.index.{Index, IndexOptions, IndexParams, IndexType}
import org.lance.index.vector.{IvfBuildParams, RQBuildParams, VectorIndexParams}
import org.lance.spark.LanceSparkReadOptions
import org.lance.spark.utils.Utils

import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Distributed build of IVF + RaBitQ vector index segments.
 *
 * <p>Each Spark task owns one contiguous run of fragments and derives the IVF centroids from those
 * fragments alone, so no global statistics are computed and no data is redistributed: the driver
 * plans, the executors read their own fragments, and the driver commits the resulting segments into
 * one named index. Segments are independent of each other, which is the same shape the Iceberg side
 * uses for per-file indexes.
 *
 * <p>Lance requires precomputed centroids for a fragment-restricted build
 * ("missing precomputed IVF centroids ... for distributed segment build"), so the task computes them
 * from the fragment data it is about to index; see [[SegmentIvfCentroids]]. One IVF partition uses
 * the segment mean, more partitions use a segment-local k-means. Either way the statistics stay a
 * per-segment decision.
 */
class VectorSegmentIndexJob(
    sc: SparkContext,
    indexName: String,
    column: String,
    dimension: Int,
    numPartitions: Int,
    numBits: Int,
    distanceType: VectorDistanceType,
    readOptions: LanceSparkReadOptions,
    fragmentBatches: Seq[List[Integer]],
    nsImpl: Option[String],
    nsProps: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOpts: Option[Map[String, String]]) {

  def run(): Seq[Index] = {
    val encodedReadOptions = encode(readOptions)

    val tasks = fragmentBatches.map { batch =>
      VectorSegmentIndexTask(
        encodedReadOptions,
        indexName,
        column,
        dimension,
        numPartitions,
        numBits,
        distanceType.sqlName,
        batch,
        nsImpl,
        nsProps,
        tableId,
        initialStorageOpts)
    }

    IndexUtils.runSegmentTasks(
      sc,
      tasks.toSeq,
      s"${IndexType.IVF_RQ.name()} index build failed. Uncommitted segments are not visible to " +
        "readers and will not affect query correctness.")(_.execute())
  }
}

/** One segment: IVF + RaBitQ over a single contiguous run of fragments. */
case class VectorSegmentIndexTask(
    encodedReadOptions: String,
    indexName: String,
    column: String,
    dimension: Int,
    numPartitions: Int,
    numBits: Int,
    distanceTypeName: String,
    fragmentIds: List[Integer],
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]]) extends Serializable {

  def execute(): String = {
    val distanceType = VectorDistanceType.parse(distanceTypeName).getOrElse {
      throw new IllegalArgumentException(s"Unsupported metric: $distanceTypeName")
    }
    val readOptions = decode[LanceSparkReadOptions](encodedReadOptions)
    val dataset = Utils.openDatasetBuilder(readOptions)
      .initialStorageOptions(initialStorageOptions.map(_.asJava).orNull)
      .runtimeNamespace(
        namespaceImpl.orNull,
        namespaceProperties.map(_.asJava).orNull,
        tableId.map(_.asJava).orNull)
      .build()

    try {
      val centroids = SegmentIvfCentroids.train(
        dataset,
        column,
        dimension,
        fragmentIds,
        numPartitions,
        distanceType)
      val ivfParams = new IvfBuildParams.Builder()
        .setNumPartitions(numPartitions)
        .setMaxIters(1)
        .setCentroids(centroids)
        .build()
      val rqParams = new RQBuildParams.Builder().setNumBits(numBits.toByte).build()
      val params: IndexParams = IndexParams
        .builder()
        .setVectorIndexParams(
          new VectorIndexParams.Builder(ivfParams)
            .setDistanceType(distanceType.toLance)
            .setRqParams(rqParams)
            .build())
        .build()

      val indexOptions = IndexOptions
        .builder(Collections.singletonList(column), IndexType.IVF_RQ, params)
        .withIndexName(indexName)
        .withFragmentIds(fragmentIds.asJava)
        .replace(true)
        .build()

      encode(dataset.createIndex(indexOptions))
    } finally {
      dataset.close()
    }
  }
}

/** Distance metric accepted by the IVF + RaBitQ build and by `VECTOR_SEARCH`. */
sealed abstract class VectorDistanceType(val sqlName: String) {
  def toLance: org.lance.index.DistanceType
}

object VectorDistanceType {
  case object L2 extends VectorDistanceType("l2") {
    override def toLance: org.lance.index.DistanceType = org.lance.index.DistanceType.L2
  }
  case object Cosine extends VectorDistanceType("cosine") {
    override def toLance: org.lance.index.DistanceType = org.lance.index.DistanceType.Cosine
  }
  case object Dot extends VectorDistanceType("dot") {
    override def toLance: org.lance.index.DistanceType = org.lance.index.DistanceType.Dot
  }

  val all: Seq[VectorDistanceType] = Seq(L2, Cosine, Dot)

  private val byName: Map[String, VectorDistanceType] =
    all.flatMap { metric =>
      val aliases = metric match {
        case L2 => Seq("l2", "euclidean")
        case Cosine => Seq("cosine")
        case Dot => Seq("dot", "ip", "inner_product")
      }
      aliases.map(_ -> metric)
    }.toMap

  def parse(value: String): Option[VectorDistanceType] =
    byName.get(value.toLowerCase(java.util.Locale.ROOT))

  /** Lance distance type for an SQL metric name; also used inside a Spark task. */
  def toLance(value: String): org.lance.index.DistanceType =
    parse(value)
      .getOrElse(throw new IllegalArgumentException(s"Unsupported metric: $value"))
      .toLance
}
