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

package io.kyligence.kap.it

import java.io.File

import io.kyligence.kap.common.{CompareSupport, JobSupport, QuerySupport, SSSource}
import io.kyligence.kap.query.{QueryConstants, QueryFetcher}
import io.netty.util.internal.ThrowableUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}
import org.apache.spark.sql.execution.utils.SchemaProcessor

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class TestQueryAndBuildFunSuite
  extends SparderBaseFunSuite
    with LocalMetadata
    with JobSupport
    with QuerySupport
    with CompareSupport
    with SSSource
    with Logging {

  override val DEFAULT_PROJECT = "default"

  case class FloderInfo(floder: String, filter: List[String] = List())

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val queryFolders = List(
    FloderInfo("sql", List("query105.sql")),
    FloderInfo("sql_lookup"),
    FloderInfo("sql_casewhen"),
    FloderInfo("sql_like"),
    FloderInfo("sql_cache"),
    FloderInfo("sql_derived"),
    FloderInfo("sql_datetime"),
    FloderInfo("sql_subquery", List("query19.sql", "query25.sql")),
    FloderInfo("sql_distinct_dim"),
    //    "sql_timestamp", no exist dir
    FloderInfo("sql_orderby"),
    FloderInfo("sql_snowflake"),
    FloderInfo("sql_topn"),
    FloderInfo("sql_join"),
    FloderInfo("sql_union"),
    FloderInfo("sql_hive"),
    //    "sql_distinct_precisel", not exist dir
    FloderInfo("sql_powerbi"),
    FloderInfo("sql_raw"),
    FloderInfo("sql_rawtable"),
    FloderInfo("sql_value"),
    FloderInfo("sql_tableau", List("query00.sql", "query24.sql", "query25.sql"))
  )

  val noneCompare = List(
    FloderInfo("sql_window"),
    FloderInfo("sql_h2_uncapable"),
    FloderInfo("sql_grouping"),
    //    FloderInfo("sql_intersect_count"),
    FloderInfo("sql_percentile"),
    FloderInfo("sql_distinct")
  )
  val tempQuery = List(
//    FloderInfo("sql_tableau", List("query00.sql", "query24.sql", "query25.sql")),
    FloderInfo("temp")
  )

  val joinTypes = List(
    "left",
    "inner"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparderEnv.skipCompute()
    build()
  }
  
  override def afterAll(): Unit = {
    SparderEnv.cleanCompute()
  }

  test("buildKylinFact") {
    var result = queryFolders
      .flatMap { folder =>
        queryFolder(folder)
      }
      .filter(_ != null)
    if (result.nonEmpty) {
      print(result)
    }
    assert(result.isEmpty)

    result = noneCompare
      .flatMap { folder =>
        queryFolderWithoutCompare(folder)
      }
      .filter(_ != null)
    print(result)
    assert(result.isEmpty)
  }

  private def queryFolder(floderInfo: FloderInfo): List[String] = {
    val futures = QueryFetcher
      .fetchQueries(QueryConstants.KAP_SQL_BASE_DIR + floderInfo.floder)
      .filter { tp =>
        !floderInfo.filter.contains(new File(tp._1).getName)
      }
      .flatMap {
        case (fileName: String, query: String) =>
          joinTypes.map { joinType =>
              val afterChangeJoin = changeJoinType(query, joinType)
              val cleanedSql = cleanSql(afterChangeJoin)

              Future[String] {
                runAndCompare(afterChangeJoin, cleanedSql, DEFAULT_PROJECT,
                  s"$joinType\n$fileName\n $query\n")
              }
            }
      }
    // scalastyle:off
    val result = Await.result(Future.sequence(futures.toList), Duration.Inf)
    // scalastyle:on
    result
  }

  private def queryFolderWithoutCompare(floderInfo: FloderInfo) = {
    val futures = QueryFetcher
      .fetchQueries(QueryConstants.KAP_SQL_BASE_DIR + floderInfo.floder)
      .filter { tp =>
        !floderInfo.filter.contains(new File(tp._1).getName)
      }
      .flatMap {
        case (fileName: String, query: String) =>
          joinTypes.map { joinType =>
            val afterChangeJoin = changeJoinType(query, joinType)

            Future[String] {
              try {
                singleQuery(afterChangeJoin, DEFAULT_PROJECT).collect()
                null
              } catch {
                case exception: Throwable =>
                  s"$fileName \n$query \n${ThrowableUtil.stackTraceToString(exception)} "
              }
            }
          }
      }
    // scalastyle:off
    val result = Await.result(Future.sequence(futures.toList), Duration.Inf)
    // scalastyle:on
    result
  }

  def build(): Unit = {
    if ("true" == System.getProperty("noBuild", "false")) {
      logInfo("Direct query")
    } else {
      if ("true" == System.getProperty("isDeveloperMode", "false")) {
        fullBuildCube("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
        fullBuildCube("741ca86a-1f13-46da-a59f-95fb68615e3a")
      } else {
        buildFourSegementAndMerge("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
        buildFourSegementAndMerge("741ca86a-1f13-46da-a59f-95fb68615e3a")
      }

      // replace metadata with new one after build
      dumpMetadata()
      SchemaProcessor.checkSchema(spark, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", DEFAULT_PROJECT)
      SchemaProcessor.checkSchema(spark, "741ca86a-1f13-46da-a59f-95fb68615e3a", DEFAULT_PROJECT)
    }

  }
}
