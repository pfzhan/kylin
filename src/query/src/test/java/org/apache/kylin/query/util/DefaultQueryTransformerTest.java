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

 
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.query.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class DefaultQueryTransformerTest {

    @Test
    public void SumOfFnConvertTransform() throws Exception {
        DefaultQueryTransformer transformer = new DefaultQueryTransformer();

        String fnConvertSumSql = "select sum({fn convert(\"LSTG_SITE_ID\", SQL_DOUBLE)}) from KYLIN_SALES group by LSTG_SITE_ID";
        String correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(\"LSTG_SITE_ID\") from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //test SQL contains blank
        //Case one blank interval
        fnConvertSumSql = "select sum ( { fn convert( \"LSTG_SITE_ID\" , SQL_DOUBLE) } ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(\"LSTG_SITE_ID\") from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //Case multi blank interval
        fnConvertSumSql = "select SUM  (  {  fn  convert(  \"LSTG_SITE_ID\"  ,  SQL_DOUBLE  )  }  ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(\"LSTG_SITE_ID\") from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //Case one or multi blank interval
        fnConvertSumSql = "select SUM(  { fn convert( \"LSTG_SITE_ID\"  , SQL_DOUBLE  ) }  ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(\"LSTG_SITE_ID\") from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //test exception case of "... fnconvert ..."
        fnConvertSumSql = "select SUM ({fnconvert(\"LSTG_SITE_ID\", SQL_DOUBLE)}) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertFalse("select sum(\"LSTG_SITE_ID\") from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //test SQL contains multi sum
        fnConvertSumSql = "select SUM({fn convert(\"LSTG_SITE_ID\", SQL_DOUBLE)}), SUM({fn convert(\"price\", SQL_DOUBLE)}) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(\"LSTG_SITE_ID\"), sum(\"price\") from KYLIN_SALES group by LSTG_SITE_ID"
                .equalsIgnoreCase(correctSql));

    }

    @Test
    public void SumOfCastTransform() throws Exception {
        DefaultQueryTransformer transformer = new DefaultQueryTransformer();

        String fnConvertSumSql = "select SUM(CAST(LSTG_SITE_ID AS DOUBLE)) from KYLIN_SALES group by LSTG_SITE_ID";
        String correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(LSTG_SITE_ID) from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //test SQL contains blank
        //Case one blank interval
        fnConvertSumSql = "select SUM ( CAST ( LSTG_SITE_ID AS DOUBLE ) ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(LSTG_SITE_ID) from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //Case multi blank interval
        fnConvertSumSql = "select  SUM (  CAST  (  LSTG_SITE_ID  AS  DOUBLE ) ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(LSTG_SITE_ID) from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //Case one or multi blank interval
        fnConvertSumSql = "select SUM (  CAST(LSTG_SITE_ID   AS      DOUBLE )  ) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(LSTG_SITE_ID) from KYLIN_SALES group by LSTG_SITE_ID".equalsIgnoreCase(correctSql));

        //test SQL contains multi sum
        fnConvertSumSql = "select SUM(CAST(LSTG_SITE_ID AS DOUBLE)), SUM(CAST(price AS DOUBLE)) from KYLIN_SALES group by LSTG_SITE_ID";
        correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select sum(LSTG_SITE_ID), sum(price) from KYLIN_SALES group by LSTG_SITE_ID"
                .equalsIgnoreCase(correctSql));
    }
    
    @Test
    public void functionEscapeTransform() throws Exception {
        DefaultQueryTransformer transformer = new DefaultQueryTransformer();

        String fnConvertSumSql = "select {fn EXTRACT(YEAR from PART_DT)} from KYLIN_SALES";
        String correctSql = transformer.transform(fnConvertSumSql, "", "");
        assertTrue("select EXTRACT(YEAR from PART_DT) from KYLIN_SALES".equalsIgnoreCase(correctSql));
    }
}