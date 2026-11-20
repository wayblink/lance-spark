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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Unit tests for [[SegmentIvfCentroids]].
 *
 * <p>Pure JVM: no Spark session, no Lance native library. The dataset-facing parts (sampling and the
 * mean path) are covered end to end by the vector index tests; these pin the clustering contract the
 * native build depends on, mainly the shape of the centroid matrix.
 */
class SegmentIvfCentroidsTest {

  @Test
  def kmeansSeparatesTwoWellSpacedClusters(): Unit = {
    val dimension = 4
    val samples = Array[Float](
      0.0f, 0.0f, 0.0f, 0.0f,
      0.2f, 0.0f, 0.0f, 0.0f,
      10.0f, 10.0f, 10.0f, 10.0f,
      10.2f, 10.0f, 10.0f, 10.0f)

    val centroids = SegmentIvfCentroids.kmeans(samples, 4, dimension, 2, 10, normalize = false)

    assertEquals(2 * dimension, centroids.length, "one row per partition")
    val means = Seq(0 until dimension, dimension until 2 * dimension)
      .map(row => row.map(centroids(_)).sum / dimension)
      .sorted
    assertTrue(means.head < 1.0f, s"one centroid must sit in the low cluster, got: $means")
    assertTrue(means.last > 9.0f, s"one centroid must sit in the high cluster, got: $means")
  }

  @Test
  def kmeansKeepsTheMatrixShapeWhenPartitionsExceedRows(): Unit = {
    val samples = Array[Float](1.0f, 2.0f, 3.0f, 4.0f)

    val centroids = SegmentIvfCentroids.kmeans(samples, 2, 2, 4, 5, normalize = false)

    assertEquals(8, centroids.length, "the native build requires partitions x dimension floats")
    assertTrue(centroids.forall(value => !value.isNaN && !value.isInfinite), "no NaN or infinity")
  }

  @Test
  def kmeansReturnsZeroCentroidsWithoutRows(): Unit = {
    val centroids = SegmentIvfCentroids.kmeans(new Array[Float](0), 0, 4, 3, 5, normalize = false)

    assertEquals(12, centroids.length)
    assertTrue(centroids.forall(_ == 0.0f), "an all-NULL segment keeps the mean path's answer")
  }

  @Test
  def normalizeProducesUnitCentroids(): Unit = {
    val samples = Array[Float](3.0f, 4.0f, 6.0f, 8.0f)

    val centroids = SegmentIvfCentroids.kmeans(samples, 2, 2, 2, 5, normalize = true)

    var row = 0
    while (row < 2) {
      val norm = math.hypot(centroids(row * 2).toDouble, centroids(row * 2 + 1).toDouble)
      assertEquals(1.0d, norm, 1e-4d, "cosine centroids must be unit vectors")
      row += 1
    }
  }

  @Test
  def sampleSizeStaysWithinTheHeapBounds(): Unit = {
    assertEquals(2048, SegmentIvfCentroids.sampleSize(1), "a tiny index still gets a usable sample")
    assertEquals(8192, SegmentIvfCentroids.sampleSize(1 << 20), "the sample is capped")
  }

  @Test
  def iterationsShrinkWhenThePartitionCountGrows(): Unit = {
    assertEquals(
      10,
      SegmentIvfCentroids.iterationsFor(2048, 4, 128),
      "a small model gets the full iteration budget")
    assertEquals(
      1,
      SegmentIvfCentroids.iterationsFor(8192, 4096, 1024),
      "a huge model falls back to a single pass")
  }
}
