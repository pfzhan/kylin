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

import io.kyligence.kap.engine.spark.mockup.external.UBSViewCatalog

class UBSViewTest extends WithKylinExternalCatalog {

  override protected def externalCatalog: String = classOf[UBSViewCatalog].getName

  protected def checkExternalCatalogBeforeAll(): Unit = {}


  val createFactTable: String =
    s"""create table fact (
       |   id bigint,
       |   dimid int,
       |   YZT_Mrgn_Sngn_Flg Boolean,
       |   Abrd_Br_Ast_Hld_Flg Boolean,
       |   Data_Dt timestamp,
       |   Marriage_Stat_Cd string,
       |   yyyy Decimal(38,0),
       |   zzzz Decimal(38,0),
       |   t0 int,
       |   t1 int,
       |   date1 date,
       |   date2 date
       | ) USING PARQUET OPTIONS ('compression'='snappy')""".stripMargin

  val createDimTable: String =
    s"""create table dim (
       |   id int,
       |   YZT_Mrgn_Sngn_Flg Boolean,
       |   Abrd_Br_Ast_Hld_Flg Boolean,
       |   Data_Dt timestamp,
       |   Marriage_Stat_Cd string,
       |   yyyy Decimal(38,0),
       |   zzzz Decimal(38,0),
       |   t0 int,
       |   t1 int,
       |   date1 date,
       |   date2 date
       | ) USING PARQUET OPTIONS ('compression'='snappy')""".stripMargin


  test("this is simple test") {
    spark.conf.set("spark.sql.view-cache-enabled", false);
    spark.sql("show databases")
    spark.sql(createFactTable)
    spark.sql(createDimTable)
    val viewName = UBSViewCatalog.VIEW_NAME

    val testSql =
      s"""select * from fact
         | inner join $viewName on fact.dimid = $viewName.id
         | inner join $viewName as Y on fact.dimid = Y.id""".stripMargin

    spark.sql(testSql)
  }
}