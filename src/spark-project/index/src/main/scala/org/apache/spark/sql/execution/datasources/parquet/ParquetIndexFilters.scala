/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.fs.FileSystem

import org.apache.spark.sql.sources._

private[parquet] case class ParquetIndexFilters(
    fs: FileSystem,
    blocks: Array[ParquetBlockMetadata]) {
  /**
   * Transform filter with provided statistics and filter.
   */
  private def transformFilter(attribute: String)
      (func: (ColumnStatistics, Option[ColumnFilterStatistics]) => Filter): Filter = {
    val references = blocks.map { block =>
      block.indexedColumns.get(attribute) match {
        case Some(columnMetadata) =>
          val stats = columnMetadata.stats
          val filter = columnMetadata.filter
          func(stats, filter)
        case None =>
          // Attribute is not indexed, return trivial filter to scan file
          Trivial(true)
      }
    }
    // If references are empty (blocks are empty) then we return Trivial(false), because only empty
    // file has 0 block metadata chunks, and therefore there is no need to scan it.
    // If references are non empty, join them with Or, all filters must be resolved at this point
    if (references.isEmpty) Trivial(false) else foldFilter(references.reduceLeft(Or))
  }

  /**
   * Recursively fold provided index filters to trivial,
   * blocks are always non-empty.
   */
  def foldFilter(filter: Filter): Filter = {
    filter match {
      case EqualTo(attribute: String, value: Any) =>
        transformFilter(attribute) { case (stats, filter) =>
          val foundInStats = stats.contains(value)
          if (foundInStats && filter.isDefined) {
            val columnFilter = filter.get
            columnFilter.readData(fs)
            Trivial(columnFilter.mightContain(value))
          } else {
            Trivial(foundInStats)
          }
        }
      case In(attribute: String, values: Array[Any]) =>
        transformFilter(attribute) { case (stats, filter) =>
          val foundInStats = values.exists(stats.contains)
          if (foundInStats && filter.isDefined) {
            val columnFilter = filter.get
            columnFilter.readData(fs)
            Trivial(values.exists(columnFilter.mightContain))
          } else {
            Trivial(foundInStats)
          }
        }
      case IsNull(attribute: String) =>
        transformFilter(attribute) { case (stats, _) =>
          Trivial(stats.hasNull)
        }
      case GreaterThan(attribute: String, value: Any) =>
        transformFilter(attribute) { case (stats, _) =>
          // we check if value is greateer than max - definitely not in range, or value is equal to
          // max, but since we want everything greater than that, this will still yield false
          Trivial(!(stats.isGreaterThanMax(value) || stats.isEqualToMax(value)))
        }
      case GreaterThanOrEqual(attribute: String, value: Any) =>
        transformFilter(attribute) { case (stats, _) =>
          // equaity value == max is valid and is in range
          Trivial(!stats.isGreaterThanMax(value))
        }
      case LessThan(attribute: String, value: Any) =>
        transformFilter(attribute) { case (stats, _) =>
          // similar to GreaterThan, we check if value is less than min or is equal to min,
          // otherwise is considered in range
          Trivial(!(stats.isLessThanMin(value) || stats.isEqualToMin(value)))
        }
      case LessThanOrEqual(attribute: String, value: Any) =>
        transformFilter(attribute) { case (stats, _) =>
          // if value is equal to min, we still have to scan file
          Trivial(!stats.isLessThanMin(value))
        }
      case And(left: Filter, right: Filter) =>
        And(foldFilter(left), foldFilter(right)) match {
          case And(Trivial(false), _) => Trivial(false)
          case And(_, Trivial(false)) => Trivial(false)
          case And(Trivial(true), right) => right
          case And(left, Trivial(true)) => left
          case other => other
        }
      case Or(left: Filter, right: Filter) =>
        Or(foldFilter(left), foldFilter(right)) match {
          case Or(Trivial(true), _) => Trivial(true)
          case Or(_, Trivial(true)) => Trivial(true)
          case Or(Trivial(false), right) => right
          case Or(left, Trivial(false)) => left
          case other => other
        }
      case Not(child: Filter) =>
        Not(foldFilter(child)) match {
          case Not(Trivial(false)) => Trivial(true)
          case Not(Trivial(true)) => Trivial(false)
          case other => other
        }
      case trivial: Trivial =>
        trivial
      case unsupportedFilter =>
        // return 'true' to scan all partitions
        // currently unsupported filters are:
        // - IsNotNull
        // - StringStartsWith
        // - StringEndsWith
        // - StringContains
        // - EqualNullSafe
        Trivial(true)
    }
  }
}
