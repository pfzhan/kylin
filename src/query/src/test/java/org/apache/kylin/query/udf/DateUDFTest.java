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

package org.apache.kylin.query.udf;

import io.kyligence.kap.junit.TimeZoneTestRunner;
import org.apache.kylin.common.HotLoadKylinPropertiesTestCase;
import org.apache.kylin.query.udf.dateUdf.DatePartUDF;
import org.apache.kylin.query.udf.dateUdf.DateTruncUDF;
import org.apache.kylin.query.udf.dateUdf.DateDiffUDF;
import org.apache.kylin.query.udf.dateUdf.AddMonthsUDF;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

@RunWith(TimeZoneTestRunner.class)
public class DateUDFTest extends HotLoadKylinPropertiesTestCase {

    /**
     * test AddMonth on different timeZone, Meanwhile data come from different timeZone too.
     * @throws Exception
     */
    @Test
    public void testAddMonthsUDF() throws Exception {
        AddMonthsUDF addMonthsUDF = new AddMonthsUDF();

        //test normal case ADD_MONTHS(date)
        Date beforeDateAddMonth = Date.valueOf("2011-01-31");
        Date afterDateAddMonth = addMonthsUDF.ADD_MONTHS(beforeDateAddMonth, 2);
        assertEquals("2011-03-31", afterDateAddMonth.toString());

        //test special case ADD_MONTHS(date)
        beforeDateAddMonth = Date.valueOf("2012-01-31");
        afterDateAddMonth = addMonthsUDF.ADD_MONTHS(beforeDateAddMonth, 1);
        assertEquals("2012-02-29", afterDateAddMonth.toString());

        //test ADD_MONTHS(date). data come from different timeZone.  
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd Z", Locale.US);
        beforeDateAddMonth = new Date(simpleDateFormat.parse("2011-01-31 +0900").getTime());
        afterDateAddMonth = addMonthsUDF.ADD_MONTHS(beforeDateAddMonth, 2);
        Date expectAfterDateAddMonth = new Date(simpleDateFormat.parse("2011-03-31 +0900").getTime());
        assertEquals(expectAfterDateAddMonth.toString(), afterDateAddMonth.toString());

        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd Z", Locale.US);
        beforeDateAddMonth = new Date(simpleDateFormat.parse("2011-01-31 +0800").getTime());
        afterDateAddMonth = addMonthsUDF.ADD_MONTHS(beforeDateAddMonth, 2);
        expectAfterDateAddMonth = new Date(simpleDateFormat.parse("2011-03-31 +0800").getTime());
        assertEquals(expectAfterDateAddMonth.toString(), afterDateAddMonth.toString());

        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd Z", Locale.US);
        beforeDateAddMonth = new Date(simpleDateFormat.parse("2011-01-31 +0700").getTime());
        afterDateAddMonth = addMonthsUDF.ADD_MONTHS(beforeDateAddMonth, 2);
        expectAfterDateAddMonth = new Date(simpleDateFormat.parse("2011-03-31 +0700").getTime());
        assertEquals(expectAfterDateAddMonth.toString(), afterDateAddMonth.toString());

        //test ADD_MONTHS(timestamp). data come from different timeZone
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
        Timestamp beforeTimestampAddMonth = new Timestamp(simpleDateFormat.parse("2011-01-31 11:49:45.256+1100").getTime());
        Timestamp expectAfterTimestampAddMonth = new Timestamp(simpleDateFormat.parse("2011-03-31 11:49:45.256+1100").getTime());
        Timestamp afterTimestampAddMonth = addMonthsUDF.ADD_MONTHS(beforeTimestampAddMonth, 2);
        assertEquals(expectAfterTimestampAddMonth.toString(), afterTimestampAddMonth.toString());

        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
        beforeTimestampAddMonth = new Timestamp(simpleDateFormat.parse("2011-01-31 11:49:45.256+0800").getTime());
        afterTimestampAddMonth = addMonthsUDF.ADD_MONTHS(beforeTimestampAddMonth, 2);
        expectAfterTimestampAddMonth = new Timestamp(simpleDateFormat.parse("2011-03-31 11:49:45.256+0800").getTime());
        assertEquals(expectAfterTimestampAddMonth.toString(), afterTimestampAddMonth.toString());

        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
        beforeTimestampAddMonth = new Timestamp(simpleDateFormat.parse("2011-01-31 11:49:45.256+0700").getTime());
        afterTimestampAddMonth = addMonthsUDF.ADD_MONTHS(beforeTimestampAddMonth, 2);
        expectAfterTimestampAddMonth = new Timestamp(simpleDateFormat.parse("2011-03-31 11:49:45.256+0700").getTime());
        assertEquals(expectAfterTimestampAddMonth.toString(), afterTimestampAddMonth.toString());

    }

    @Test
    public void testDateDiffUDF() throws Exception {
        DateDiffUDF dateDiffUDF = new DateDiffUDF();
        assertEquals(-35, dateDiffUDF.DATEDIFF(Date.valueOf("2019-06-28"), Date.valueOf("2019-08-02")));
    }

    @Test
    public void testDatePartUDF() throws Exception {
        DatePartUDF datePartUDF = new DatePartUDF();
        assertEquals(2019, datePartUDF.DATE_PART("year", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(5, datePartUDF.DATE_PART("month", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(9, datePartUDF.DATE_PART("day", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(11, datePartUDF.DATE_PART("hour", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(49, datePartUDF.DATE_PART("minute", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(45, datePartUDF.DATE_PART("seconds", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(2019, datePartUDF.DATE_PART("year", Date.valueOf("2019-05-09")));
        assertEquals(5, datePartUDF.DATE_PART("month", Date.valueOf("2019-05-09")));
        assertEquals(9, datePartUDF.DATE_PART("day", Date.valueOf("2019-05-09")));
    }

    @Test
    public void testDateTruncUDF() throws Exception {
        DateTruncUDF dateTruncUDF = new DateTruncUDF();
        assertEquals(2019, dateTruncUDF.DATE_TRUNC("year", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(5, dateTruncUDF.DATE_TRUNC("month", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(9, dateTruncUDF.DATE_TRUNC("day", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(11, dateTruncUDF.DATE_TRUNC("hour", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(49, dateTruncUDF.DATE_TRUNC("minute", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(45, dateTruncUDF.DATE_TRUNC("seconds", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(2019, dateTruncUDF.DATE_TRUNC("year", Date.valueOf("2019-05-09")));
        assertEquals(5, dateTruncUDF.DATE_TRUNC("month", Date.valueOf("2019-05-09")));
        assertEquals(9, dateTruncUDF.DATE_TRUNC("day", Date.valueOf("2019-05-09")));
    }
}
