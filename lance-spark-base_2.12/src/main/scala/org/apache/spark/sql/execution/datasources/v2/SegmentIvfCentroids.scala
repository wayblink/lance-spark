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

import org.apache.arrow.vector.{Float4Vector, Float8Vector, ValueVector}
import org.apache.arrow.vector.complex.FixedSizeListVector
import org.lance.Dataset
import org.lance.ipc.ScanOptions
import org.slf4j.LoggerFactory

import java.util.Collections

import scala.collection.JavaConverters._

/**
 * IVF centroids of one index segment, trained from that segment's own fragments only.
 *
 * <p>The connector deliberately does no global statistics: every segment is built by one Spark task
 * that reads its own fragments, derives its own model and produces one index file. A
 * fragment-restricted Lance build cannot train by itself (Lance requires precomputed centroids for a
 * distributed segment build), so this object computes the `num_partitions x dimension` centroid
 * matrix the build needs.
 *
 * <p>One partition keeps the cheap and exact route: the centroid is the mean of the segment's
 * vectors, which costs one streaming pass and no clustering. More partitions sample the segment
 * (reservoir sampling, so the sample stays uniform even when the fragments are not ordered randomly)
 * and run Lloyd's algorithm with a fixed seed, so rebuilding the same data reproduces the same
 * model.
 */
private[v2] object SegmentIvfCentroids {

  private val LOG = LoggerFactory.getLogger(getClass)

  /** Rows the sample aims for per IVF partition. */
  private val TrainRowsPerPartition = 8

  /** Sampling floor: a small segment still gets enough rows to place a few centroids. */
  private val MinTrainRows = 2048

  /** Sampling ceiling: a large partition count must not turn into an unbounded heap. */
  private val MaxTrainRows = 8192

  /** Lloyd iterations are capped; the sample size, not the iteration count, is the main lever. */
  private val MaxIterations = 10

  /**
   * Distance computations (`rows x partitions x dimension`) allowed per segment.
   *
   * <p>This is what keeps a large `num_partitions` from turning a distributed build into a
   * single-threaded training job: the iteration count drops to one when the sample is big relative
   * to the budget, which still produces a usable partition layout because the centroids are seeded
   * from the data itself.
   */
  private val WorkBudget = 8.0e9

  /** Fixed seed: the same segment data must produce the same centroids on every rebuild. */
  private val Seed = 0x5DEECE66DL

  /** Rows sampled to train one segment with `numPartitions` IVF partitions. */
  def sampleSize(numPartitions: Int): Int =
    math.min(MaxTrainRows, math.max(MinTrainRows, TrainRowsPerPartition * numPartitions))

  /** Lloyd iterations affordable for this sample and partition count. */
  def iterationsFor(rows: Int, numPartitions: Int, dimension: Int): Int = {
    val work = rows.toDouble * numPartitions.toDouble * dimension.toDouble
    if (work <= 0.0) {
      MaxIterations
    } else {
      math.max(1, math.min(MaxIterations, math.floor(WorkBudget / work).toInt))
    }
  }

  /**
   * Centroids for one segment, flattened as the `num_partitions x dimension` row-major matrix Lance
   * expects.
   */
  def train(
      dataset: Dataset,
      column: String,
      dimension: Int,
      fragmentIds: List[Integer],
      numPartitions: Int,
      distanceType: VectorDistanceType): Array[Float] = {
    if (numPartitions <= 1) {
      return mean(dataset, column, dimension, fragmentIds)
    }

    // Cosine stores direction, not magnitude: normalising the sample makes squared L2 rank the same
    // way cosine does, so the same Lloyd loop applies to both metrics.
    val normalize = distanceType == VectorDistanceType.Cosine
    val (samples, rows) =
      reservoirSample(dataset, column, dimension, fragmentIds, sampleSize(numPartitions), normalize)
    if (rows == 0) {
      // Every vector in the segment is NULL. The mean path answers all zeros for that case; keep the
      // same shape here so the native build sees the matrix it was promised.
      new Array[Float](numPartitions * dimension)
    } else {
      kmeans(
        samples,
        rows,
        dimension,
        numPartitions,
        iterationsFor(rows, numPartitions, dimension),
        normalize)
    }
  }

  /**
   * Lloyd's algorithm over `rows` samples of `dimension` floats, seeded by picking rows at random
   * without replacement.
   *
   * <p>Returns `numPartitions x dimension` centroids in row-major order. When there are fewer rows
   * than partitions the sample rows are reused so the matrix still has the promised shape; the
   * resulting empty partitions cost a lookup, not correctness.
   */
  def kmeans(
      samples: Array[Float],
      rows: Int,
      dimension: Int,
      numPartitions: Int,
      iterations: Int,
      normalize: Boolean): Array[Float] = {
    require(dimension > 0, s"dimension must be positive, got: $dimension")
    require(numPartitions > 0, s"numPartitions must be positive, got: $numPartitions")
    val centroids = new Array[Float](numPartitions * dimension)
    if (rows <= 0) {
      return centroids
    }

    val random = new java.util.Random(Seed)
    if (rows >= numPartitions) {
      val order = Array.tabulate(rows)(i => i)
      var centroidsDone = 0
      while (centroidsDone < numPartitions) {
        val pick = centroidsDone + random.nextInt(rows - centroidsDone)
        val swap = order(centroidsDone)
        order(centroidsDone) = order(pick)
        order(pick) = swap
        System.arraycopy(
          samples,
          order(centroidsDone) * dimension,
          centroids,
          centroidsDone * dimension,
          dimension)
        centroidsDone += 1
      }
    } else {
      var centroidsDone = 0
      while (centroidsDone < numPartitions) {
        System.arraycopy(
          samples,
          (centroidsDone % rows) * dimension,
          centroids,
          centroidsDone * dimension,
          dimension)
        centroidsDone += 1
      }
    }

    val assignment = new Array[Int](rows)
    java.util.Arrays.fill(assignment, -1)
    val sums = new Array[Float](numPartitions * dimension)
    val counts = new Array[Int](numPartitions)

    var iteration = 0
    var changed = true
    while (iteration < iterations && changed) {
      java.util.Arrays.fill(sums, 0.0f)
      java.util.Arrays.fill(counts, 0)
      changed = false

      var row = 0
      while (row < rows) {
        val rowOffset = row * dimension
        var best = 0
        var bestDistance = Float.MaxValue
        var centroid = 0
        while (centroid < numPartitions) {
          val distance =
            squaredDistance(samples, rowOffset, centroids, centroid * dimension, dimension)
          if (distance < bestDistance) {
            bestDistance = distance
            best = centroid
          }
          centroid += 1
        }
        if (assignment(row) != best) {
          assignment(row) = best
          changed = true
        }
        counts(best) += 1
        val sumOffset = best * dimension
        var i = 0
        while (i < dimension) {
          sums(sumOffset + i) += samples(rowOffset + i)
          i += 1
        }
        row += 1
      }

      var centroid = 0
      while (centroid < numPartitions) {
        val count = counts(centroid)
        if (count > 0) {
          val scale = 1.0f / count
          val offset = centroid * dimension
          var i = 0
          while (i < dimension) {
            centroids(offset + i) = sums(offset + i) * scale
            i += 1
          }
        }
        centroid += 1
      }
      iteration += 1
    }

    if (normalize) {
      normalizeRows(centroids, numPartitions, dimension)
    }
    centroids
  }

  /** Squared L2 distance between two `dimension`-long slices of the same flat array. */
  private def squaredDistance(
      values: Array[Float],
      leftOffset: Int,
      other: Array[Float],
      rightOffset: Int,
      dimension: Int): Float = {
    var sum = 0.0f
    var i = 0
    while (i < dimension) {
      val diff = values(leftOffset + i) - other(rightOffset + i)
      sum += diff * diff
      i += 1
    }
    sum
  }

  /** One centroid from the segment's vectors: exact, streaming, no clustering. */
  private def mean(
      dataset: Dataset,
      column: String,
      dimension: Int,
      fragmentIds: List[Integer]): Array[Float] = {
    val sums = new Array[Double](dimension)
    var rows = 0L
    scan(dataset, column, dimension, fragmentIds, normalize = false) { values =>
      var i = 0
      while (i < dimension) {
        sums(i) += values(i)
        i += 1
      }
      rows += 1
    }

    val centroids = new Array[Float](dimension)
    if (rows > 0) {
      var i = 0
      while (i < dimension) {
        centroids(i) = (sums(i) / rows).toFloat
        i += 1
      }
    }
    centroids
  }

  /**
   * Uniform sample of `cap` rows (reservoir sampling, algorithm R).
   *
   * <p>Reservoir sampling rather than "the first N rows": fragments hold whatever order the writer
   * produced, and a segment's first rows can easily all come from one cluster of the data.
   */
  private def reservoirSample(
      dataset: Dataset,
      column: String,
      dimension: Int,
      fragmentIds: List[Integer],
      cap: Int,
      normalize: Boolean): (Array[Float], Int) = {
    val samples = new Array[Float](cap * dimension)
    val random = new java.util.Random(Seed)
    var filled = 0
    var seen = 0L
    scan(dataset, column, dimension, fragmentIds, normalize) { values =>
      if (filled < cap) {
        System.arraycopy(values, 0, samples, filled * dimension, dimension)
        filled += 1
      } else {
        val slot = (random.nextDouble() * (seen + 1)).toLong
        if (slot < cap) {
          System.arraycopy(values, 0, samples, slot.toInt * dimension, dimension)
        }
      }
      seen += 1
    }
    (samples, filled)
  }

  /**
   * Streams the vector column of `fragmentIds`, skipping rows that carry no embedding.
   *
   * <p>A NULL vector can come back in two shapes: the list slot itself is NULL, or the slot is
   * valid while its elements carry no value (the shape some backends materialize). Both mean the
   * same thing to a centroid, and a build that only skips the first shape fails the whole segment
   * on such a table, so a row is consumed only when every element has a value.
   */
  private def scan(
      dataset: Dataset,
      column: String,
      dimension: Int,
      fragmentIds: List[Integer],
      normalize: Boolean)(consume: Array[Float] => Unit): Unit = {
    val scanOptions = new ScanOptions.Builder()
      .columns(Collections.singletonList(column))
      .fragmentIds(fragmentIds.asJava)
      .build()
    val scanner = dataset.newScan(scanOptions)
    val reader = scanner.scanBatches()
    val values = new Array[Float](dimension)
    var skippedNulls = 0L
    try {
      while (reader.loadNextBatch()) {
        val vector: ValueVector = reader.getVectorSchemaRoot.getVector(0)
        val list = vector match {
          case fixed: FixedSizeListVector => fixed
          case other =>
            throw new UnsupportedOperationException(
              s"Cannot build a vector index on column '$column': expected a fixed-size list of " +
                s"floats, got ${other.getField.getType}")
        }
        if (list.getListSize != dimension) {
          throw new IllegalArgumentException(
            s"Cannot build a vector index on column '$column': declared dimension $dimension does " +
              s"not match the data's list size ${list.getListSize}")
        }
        val child = list.getDataVector
        val valueCount = list.getValueCount
        var row = 0
        while (row < valueCount) {
          val base = row * dimension
          var present = !list.isNull(row)
          if (present) {
            child match {
              case f4: Float4Vector =>
                var i = 0
                while (i < dimension) {
                  if (f4.isNull(base + i)) {
                    present = false
                  } else {
                    values(i) = f4.get(base + i)
                  }
                  i += 1
                }
              case f8: Float8Vector =>
                var i = 0
                while (i < dimension) {
                  if (f8.isNull(base + i)) {
                    present = false
                  } else {
                    values(i) = f8.get(base + i).toFloat
                  }
                  i += 1
                }
              case other =>
                throw new UnsupportedOperationException(
                  s"Cannot build a vector index on column '$column': unsupported element type " +
                    s"${other.getField.getType}")
            }
          }
          if (present) {
            if (normalize) {
              normalizeInPlace(values, dimension)
            }
            consume(values)
          } else {
            skippedNulls += 1
          }
          row += 1
        }
      }
      if (skippedNulls > 0) {
        LOG.warn(
          "Column '{}': skipped {} row(s) without an embedding while training segment centroids " +
            "from fragments {}",
          column,
          String.valueOf(skippedNulls),
          fragmentIds)
      }
    } finally {
      reader.close()
      scanner.close()
    }
  }

  private def normalizeInPlace(values: Array[Float], dimension: Int): Unit = {
    var sum = 0.0d
    var i = 0
    while (i < dimension) {
      val value = values(i)
      sum += value.toDouble * value
      i += 1
    }
    if (sum > 0.0d) {
      val scale = (1.0d / math.sqrt(sum)).toFloat
      i = 0
      while (i < dimension) {
        values(i) *= scale
        i += 1
      }
    }
  }

  private def normalizeRows(rows: Array[Float], count: Int, dimension: Int): Unit = {
    var row = 0
    while (row < count) {
      var sum = 0.0d
      var i = 0
      while (i < dimension) {
        val value = rows(row * dimension + i)
        sum += value.toDouble * value
        i += 1
      }
      if (sum > 0.0d) {
        val scale = (1.0d / math.sqrt(sum)).toFloat
        i = 0
        while (i < dimension) {
          rows(row * dimension + i) *= scale
          i += 1
        }
      }
      row += 1
    }
  }
}
