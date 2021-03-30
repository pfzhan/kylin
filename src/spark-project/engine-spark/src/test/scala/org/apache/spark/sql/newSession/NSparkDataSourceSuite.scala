/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
 */
package org.apache.spark.sql.newSession

import java.util.Locale

import com.google.common.collect.Maps
import io.kyligence.kap.engine.spark.NSparkCubingEngine.NSparkCubingSource
import io.kyligence.kap.engine.spark.builder.CreateFlatTable
import io.kyligence.kap.engine.spark.source.NSparkDataSource
import io.kyligence.kap.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataflowManager}
import io.kyligence.kap.metadata.model.NTableMetadataManager
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.kylin.source.SourceFactory
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.util.SparderTypeUtil
import org.junit.Ignore

import scala.collection.JavaConverters._


/**
 * Equivalence [[io.kyligence.kap.engine.spark.mockup.CsvSourceTest]]
 */
class NSparkDataSourceSuite extends SQLTestUtils with WithKylinExternalCatalog {

  private val DEFAULT_TABLE = "DEFAULT.TEST_KYLIN_FACT"
  private val project = "default"
  private val database = "default"

  test("testSourceMetadataExplorer") {
    val sparkCsvSource = new NSparkDataSource(null)
    val metaSource = sparkCsvSource.getSourceMetadataExplorer
    val databases = metaSource.listDatabases.asScala
    assert(databases.size >= 3) // at least 3
    assert(databases.contains(database.toUpperCase(Locale.ROOT)))
    val tables = metaSource.listTables(database.toUpperCase(Locale.ROOT)).asScala.map(_.toUpperCase(Locale.ROOT))
    val table = DEFAULT_TABLE.split("\\.")(1)
    assert(tables.contains(table))

    val tableDescTableExtDescPair = metaSource.loadTableMetadata(database, table, project)
    val tableDesc = tableDescTableExtDescPair.getFirst
    val readableTable = sparkCsvSource.createReadableTable(tableDesc)
    assert(readableTable.exists())
  }

  test("testGetTable") {
    overwriteSystemProp("kylin.external.catalog.mockup.sleep-interval", "1s")
    val tableMgr = NTableMetadataManager.getInstance(kylinConf, project)
    val fact = tableMgr.getTableDesc(DEFAULT_TABLE)
    val colDescs = fact.getColumns
    val sparkCsvSource = new NSparkDataSource(null)
    val cubingSource = sparkCsvSource.adaptToBuildEngine(classOf[NSparkCubingSource])
    val df = cubingSource.getSourceData(fact, spark, Maps.newHashMap[String, String]())
    assertResult(10)(df.take(10).length)
    val schema = df.schema
    for (i <- 0 until colDescs.length) {
      val field = schema.fields(i)
      assertResult(colDescs(i).getName)(field.name)
      assertResult(SparderTypeUtil.toSparkType(colDescs(i).getType))(field.dataType)
    }
  }

  test("testGetSegmentRange") {
    val sparkCsvSource = new NSparkDataSource(null)
    val segmentRange = sparkCsvSource.getSegmentRange("0", "21423423")
    assert(segmentRange.isInstanceOf[SegmentRange.TimePartitionedSegmentRange])
    assertResult(0L)(segmentRange.getStart)
    assertResult(21423423L)(segmentRange.getEnd)

    val segmentRange2 = sparkCsvSource.getSegmentRange("", "")
    assert(segmentRange2.isInstanceOf[SegmentRange.TimePartitionedSegmentRange])
    assertResult(0L)(segmentRange2.getStart)
    assertResult(Long.MaxValue)(segmentRange2.getEnd)
  }

  test("testGetFlatTable") {
    log.info(kylinConf.getMetadataUrl.toString)
    val dsMgr = NDataflowManager.getInstance(kylinConf, project)
    val dataflow = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
    val model = dataflow.getModel
    assertResult(classOf[NSparkDataSource])(SourceFactory.getSource(model.getRootFactTable.getTableDesc).getClass)

    val flatTableDesc =
      new NCubeJoinedFlatTableDesc(dataflow.getIndexPlan,
        new SegmentRange.TimePartitionedSegmentRange(0L, System.currentTimeMillis), true)

    val flatTable = new CreateFlatTable(flatTableDesc, null, null, spark, null)
    val ds = flatTable.generateDataset(needEncode = false, needJoin = true)

    assertResult(10)(ds.take(10).length)

    ds.schema
      .foreach(field => assert(null != model.findColumn(model.getColumnNameByColumnId(Integer.parseInt(field.name)))))

    val cols = dataflow.getIndexPlan.getEffectiveDimCols.keySet.asScala.toList
      .map(id => new ColumnName(String.valueOf(id)))
    assertResult(10)(ds.select(cols: _*).take(10).length)
  }
}
