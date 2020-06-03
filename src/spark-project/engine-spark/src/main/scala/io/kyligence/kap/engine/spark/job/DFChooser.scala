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

package io.kyligence.kap.engine.spark.job

import com.google.common.base.Preconditions
import com.google.common.collect.{Lists, Maps}
import io.kyligence.kap.engine.spark.builder._
import io.kyligence.kap.engine.spark.utils.SparkDataSource._
import io.kyligence.kap.metadata.cube.cuboid.{NCuboidLayoutChooser, NSpanningTree}
import io.kyligence.kap.metadata.cube.model._
import io.kyligence.kap.metadata.model.NDataModel
import io.kyligence.kap.metadata.model.NDataModel.TableKind
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.internal.Logging
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql._

import scala.collection.JavaConverters._
import scala.collection.mutable

class DFChooser(toBuildTree: NSpanningTree,
                var seg: NDataSegment,
                jobId: String,
                ss: SparkSession,
                config: KylinConfig,
                needEncoding: Boolean)
  extends Logging {
  var reuseSources: java.util.Map[java.lang.Long, NBuildSourceInfo] =
    Maps.newHashMap[java.lang.Long, NBuildSourceInfo]()
  var flatTableSource: NBuildSourceInfo = _
  val flatTableDesc =
    new NCubeJoinedFlatTableDesc(seg.getIndexPlan, seg.getSegRange, DFChooser.needJoinLookupTables(seg.getModel, toBuildTree))

  @throws[Exception]
  def decideSources(): Unit = {
    var map = Map.empty[Long, NBuildSourceInfo]
    toBuildTree.getRootIndexEntities.asScala
      .foreach { desc =>
        val layout = NCuboidLayoutChooser.selectLayoutForBuild(seg, desc)

        if (layout != null) {
          if (map.contains(layout.getId)) {
            map.apply(layout.getId).addCuboid(desc)
          } else {
            val nBuildSourceInfo = getSourceFromLayout(layout, desc)
            map += (layout.getId -> nBuildSourceInfo)
          }
        } else {
          if (flatTableSource == null) {
            if (needEncoding) {
              if (seg.isSnapshotReady) {
                logInfo(s"Skip already built snapshot, segment: ${seg.getId} of dataflow: ${seg.getDataflow.getId}")
              } else {
                val snapshotBuilder = new DFSnapshotBuilder(seg, ss)
                snapshotBuilder.buildSnapshot
              }
            }
            flatTableSource = getFlatTable()
          }
          flatTableSource.addCuboid(desc)
        }
      }
    map.foreach(entry => reuseSources.put(entry._1, entry._2))
  }

  def computeColumnBytes(): mutable.HashMap[String, Long] = {
    val df = flatTableSource.getFlattableDS
    val cols = flatTableSource.getAllColumns
    val columns = df.columns
    val length = columns.length
    val result = new mutable.HashMap[String, Long]
    val rows = config.getCapacitySampleRows
    df.take(rows).foreach(row => {
      var i = 0
      for (i <- 0 until length - 1) {
        val columnName = cols.get(i).getCanonicalName
        val value = row.get(i)
        val strValue = if (value == null) null
        else value.toString
        val bytes = DFChooser.utf8Length(strValue)
        if (result.get(columnName).isEmpty) result(columnName) = bytes
        else result(columnName) = bytes + result(columnName)
      }
    })
    result
  }

  def persistFlatTableIfNecessary(): String = {
    var path = ""
    if (shouldPersistFlatTable()) {
      val df = flatTableSource.getFlattableDS
      val columns = new mutable.ListBuffer[String]
      val columnIndex = df.schema.fieldNames.zipWithIndex.map(tp => (tp._2, tp._1)).toMap
      flatTableSource.getToBuildCuboids.asScala.foreach { c =>
        columns.appendAll(c.getDimensions.asScala.map(_.toString))

        c.getEffectiveMeasures.asScala.foreach { mea =>
          val parameters = mea._2.getFunction.getParameters.asScala
          if (parameters.head.isColumnType) {
            val measureUsedCols = parameters.map(p => columnIndex.apply(flatTableDesc.getColumnIndex(p.getColRef)).toString)
            columns.appendAll(measureUsedCols)
          }
        }
      }
      val toBuildCuboidColumns = columns.distinct.sorted
      logInfo(s"to build cuboid columns are $toBuildCuboidColumns")
      if (df.schema.nonEmpty) {
        val selectedColumns = flatTableDesc.getIndices.asScala.sorted.map(_.toString)
        path = s"${config.getFlatTableDir(seg.getProject, seg.getDataflow.getId, seg.getId)}"
        if (!shouldUpdateFlatTable(new Path(path), selectedColumns)) {
          logInfo(s"Skip already persisted flat table, segment: ${seg.getId} of dataflow: ${seg.getDataflow.getId}")
        } else {
          ss.sparkContext.setJobDescription("Persist flat table.")
          df.write.mode(SaveMode.Overwrite).parquet(path)
          // checkpoint
          val oldSelectedColumns = seg.getSelectedColumns.asScala
          logInfo(s"Persist flat table into: $path. " +
            s"Selected columns in table are [${selectedColumns.mkString(",")}]. " +
            s"Old Selected columns in table are [${oldSelectedColumns.mkString(",")}].")
          DFBuilderHelper.checkPointSegment(seg, (copied: NDataSegment) => {
            copied.setFlatTableReady(true)
            copied.setSelectedColumns(selectedColumns.toList.asJava)
          })
        }
        flatTableSource.setParentStorageDF(ss.read.parquet(path))
      }
    }
    path
  }

  private def persistFactViewIfNecessary(): String = {
    var path = ""
    if (needEncoding && config.isPersistFlatViewEnabled) {
      logInfo(s"Check project:${seg.getProject} seg:${seg.getName} persist view fact table.")
      val fact = flatTableDesc.getDataModel.getRootFactTable
      val globalDicts = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(seg, toBuildTree)
      val existsFactDictCol = globalDicts.asScala.exists(_.getTableRef.getTableIdentity.equals(fact.getTableIdentity))

      logInfo(s"Fact table ${fact.getAlias} isView ${fact.getTableDesc.isView} and fact table exists dict col ${existsFactDictCol}")
      if (fact.getTableDesc.isView && existsFactDictCol) {
        var viewDS = ss.table(fact.getTableDesc).alias(fact.getAlias)
        path = s"${config.getJobTmpViewFactTableDir(seg.getProject, jobId)}"

        if (seg.isFactViewReady && HadoopUtil.getWorkingFileSystem.exists(new Path(path))) {
          logInfo(s"Skip already persisted fact view, segment: ${seg.getId} of dataflow: ${seg.getDataflow.getId}")
        } else {
          ss.sparkContext.setJobDescription("Persist view fact table.")
          viewDS = FlatTableHelper.applyPartitionDesc(flatTableDesc, viewDS, false)
          viewDS = FlatTableHelper.applyFilterCondition(flatTableDesc, viewDS, false)
          viewDS.write.mode(SaveMode.Overwrite).parquet(path)
          logInfo(s"Persist view fact table into:$path.")

          // checkpoint fact view
          DFBuilderHelper.checkPointSegment(seg, (copied: NDataSegment) => copied.setFactViewReady(true))
        }
        // end of fact view persist
      }
    }
    path
  }

  private def getSourceFromLayout(layout: LayoutEntity,
                                  indexEntity: IndexEntity) = {
    val buildSource = new NBuildSourceInfo
    val segDetails = seg.getSegDetails
    val dataCuboid = segDetails.getLayoutById(layout.getId)
    Preconditions.checkState(dataCuboid != null)
    buildSource.setParentStorageDF(StorageStoreUtils.toDF(seg, layout, ss))
    buildSource.setSparkSession(ss)
    buildSource.setCount(dataCuboid.getRows)
    buildSource.setLayoutId(layout.getId)
    buildSource.setByteSize(dataCuboid.getByteSize)
    buildSource.addCuboid(indexEntity)
    logInfo(
      s"Reuse a suitable layout: ${layout.getId} for building cuboid: ${indexEntity.getId}")
    buildSource
  }

  @throws[Exception]
  private def getFlatTable(): NBuildSourceInfo = {
    val viewPath = persistFactViewIfNecessary()
    val sourceInfo = new NBuildSourceInfo
    sourceInfo.setSparkSession(ss)
    sourceInfo.setLayoutId(DFChooser.FLAT_TABLE_FLAG)
    sourceInfo.setViewFactTablePath(viewPath)

    val needJoin = DFChooser.needJoinLookupTables(seg.getModel, toBuildTree)
    val flatTableDesc = new NCubeJoinedFlatTableDesc(seg.getIndexPlan, seg.getSegRange, needJoin)
    val flatTable = new CreateFlatTable(flatTableDesc, seg, toBuildTree, ss, sourceInfo)
    val afterJoin: Dataset[Row] = flatTable.generateDataset(needEncoding, needJoin)
    sourceInfo.setFlattableDS(afterJoin)
    sourceInfo.setAllColumns(flatTableDesc.getAllColumns)
    sourceInfo
  }

  def shouldPersistFlatTable(): Boolean = {
    if (flatTableSource == null) {
      false
    } else if (config.isPersistFlatTableEnabled) {
      true
    } else if (flatTableSource.getToBuildCuboids.size() > config.getPersistFlatTableThreshold) {
      true
    } else {
      false
    }
  }

  def shouldUpdateFlatTable(flatTablePath: Path, selectedColumns: Seq[String]): Boolean = {
    if (seg.isFlatTableReady && HadoopUtil.getWorkingFileSystem.exists(flatTablePath)) {
      val newSelectedColumns = if (selectedColumns == null) Seq.empty[String]
      else selectedColumns.filterNot(seg.getSelectedColumns.contains(_)).distinct
      if (newSelectedColumns.isEmpty) {
        logInfo(s"Already persisted flat table found without new selected columns," +
          s" segment: ${seg.getId}, dataFlow: ${seg.getDataflow.getId}")
        false
      } else {
        logInfo(s"Already persisted flat table found with new selected columns: [${newSelectedColumns.mkString(",")}]," +
          s" segment: ${seg.getId}, dataFlow: ${seg.getDataflow.getId}")
        true
      }
    } else {
      true
    }
  }
}

object DFChooser extends Logging {
  def apply(toBuildTree: NSpanningTree,
            seg: NDataSegment,
            jobId: String,
            ss: SparkSession,
            config: KylinConfig,
            needEncoding: Boolean): DFChooser =
    new DFChooser(toBuildTree: NSpanningTree,
      seg: NDataSegment,
      jobId,
      ss: SparkSession,
      config: KylinConfig,
      needEncoding)

  val FLAT_TABLE_FLAG: Long = -1L

  def utf8Length(sequence: CharSequence): Int = {
    var count = 0
    var i = 0
    if (sequence != null) {
      val len = sequence.length
      while (i < len) {
        val ch = sequence.charAt(i)
        if (ch <= 0x7F) count += 1
        else if (ch <= 0x7FF) count += 2
        else if (Character.isHighSurrogate(ch)) {
          count += 4
          i += 1
        }
        else count += 3
        i += 1
      }
    }
    count
  }


  def needJoinLookupTables(model: NDataModel, toBuildTree: NSpanningTree): Boolean = {
    val conf = KylinConfig.getInstanceFromEnv
    if (!conf.isFlatTableJoinWithoutLookup) {
      return true
    }

    if (StringUtils.isNotBlank(model.getFilterCondition)) {
      return true
    }

    val joinTables = model.getJoinTables
    val factTable = model.getRootFactTable
    var needJoin = false
    if (joinTables.asScala.filter(_.getJoin.isLeftJoin).size != joinTables.size()) {
      needJoin = true
    }

    if (joinTables.asScala.filter(_.getKind == TableKind.LOOKUP).size != joinTables.size()) {
      needJoin = true
    }

    val toBuildCols = Lists.newArrayList[TblColRef]()
    toBuildTree.getRootIndexEntities.asScala.map(
      index => {
        index.getEffectiveMeasures.asScala.map(
          mea =>
            toBuildCols.addAll(mea._2.getFunction.getColRefs)
        )
        index.getEffectiveDimCols.asScala.map(
          dim =>
            toBuildCols.add(dim._2)
        )
      }
    )
    toBuildCols.asScala.distinct.map(
      col =>
        if (!factTable.getTableIdentity.equalsIgnoreCase(col.getTableRef.getTableIdentity)) {
          needJoin = true
        }
    )

    needJoin
  }
}
