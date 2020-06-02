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
package io.kyligence.kap.engine.spark.source

import io.kyligence.kap.common.util.TempMetadataBuilder
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types._

class NSparkTableMetaExplorerTest extends SparderBaseFunSuite with SharedSparkSession {

  conf.set("spark.sql.catalogImplementation", "hive")
  test("Test load error view") {
    System.setProperty("KYLIN_CONF", TempMetadataBuilder.N_SPARK_PROJECT_KAP_META_TEST_DATA)
    SparderEnv.setSparkSession(spark)

    spark.sql("CREATE TABLE hive_table (a int, b int)")

    withTable("hive_table") {

      val view = CatalogTable(
        identifier = TableIdentifier("view"),
        tableType = CatalogTableType.VIEW,
        storage = CatalogStorageFormat.empty,
        schema = new StructType().add("a", "double").add("b", "int"),
        viewText = Some("SELECT 1.0 AS `a`, `b` FROM hive_table AS gen_subquery_0")
      )
      spark.sessionState.catalog.createTable(view, ignoreIfExists = false)

      withView("view") {
        val message = intercept[RuntimeException](new NSparkTableMetaExplorer().getSparkTableMeta("", "view")).getMessage
        assert(message.contains("Error for parser view: "))
      }
    }
    System.clearProperty("KYLIN_CONF")
  }

  test("Test load hive type") {
    SparderEnv.setSparkSession(spark)

    val view = CatalogTable(
      identifier = TableIdentifier("hive_table_types"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("a", "string", nullable = true, new MetadataBuilder().putString("HIVE_TYPE_STRING", "char(10)").build())
        .add("b", "string", nullable = true, new MetadataBuilder().putString("HIVE_TYPE_STRING", "varchar(33)").build())
        .add("c", "int"),
      properties = Map()
    )
    spark.sessionState.catalog.createTable(view, ignoreIfExists = false)

    withTable("hive_table_types") {
      val meta = new NSparkTableMetaExplorer().getSparkTableMeta("", "hive_table_types")
      assert(meta.allColumns.get(0).dataType == "char(10)")
      assert(meta.allColumns.get(1).dataType == "varchar(33)")
      assert(meta.allColumns.get(2).dataType == "int")
    }
  }

  test("Test load hive type with unsupported type array") {
    importUnsupportedCol(ArrayType(LongType))
  }

  test("Test load hive type with unsupported type map") {
    importUnsupportedCol(MapType(StringType, StringType))
  }

  test("Test load hive type with unsupported type struct") {
    importUnsupportedCol(new StructType().add("map", MapType(StringType, StringType)))
  }

  test("Test load hive type with unsupported type binary") {
    importUnsupportedCol(BinaryType)
  }

  def createTmpCatalog(st: StructType): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier("hive_table_types"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      st,
      properties = Map()
    )
  }

  def importUnsupportedCol(unsupported: DataType): Unit = {
    SparderEnv.setSparkSession(spark)

    val st = new StructType()
      .add("c", "int")
      .add("d", unsupported)
    val catalogTable = createTmpCatalog(st)
    spark.sessionState.catalog.createTable(catalogTable, ignoreIfExists = false)

    withTable("hive_table_types") {
      val res = new NSparkTableMetaExplorer().getSparkTableMeta("", "hive_table_types")
      assert(res.allColumns.size() == 1)
      assert(res.allColumns.get(0).name.equals("c"))
    }
  }
}
