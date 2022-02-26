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

import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.kylin.query.udf.dateUdf.DateDiffUDF;
import org.apache.kylin.query.udf.dateUdf.DatePartUDF;

import io.kyligence.kap.junit.annotation.MultiTimezoneTest;

public class DateUDFTest {

    @MultiTimezoneTest(timezones = { "GMT+8", "GMT+12", "GMT+0" })
    public void testDateDiffUDF() throws Exception {
        DateDiffUDF dateDiffUDF = new DateDiffUDF();
        assertEquals(-35, (long) dateDiffUDF.DATEDIFF(Date.valueOf("2019-06-28"), Date.valueOf("2019-08-02")));
    }

    @MultiTimezoneTest(timezones = { "GMT+8", "GMT+12", "GMT+0" })
    public void testDatePartUDF() throws Exception {
        DatePartUDF datePartUDF = new DatePartUDF();
        assertEquals(2019, (long) datePartUDF.DATE_PART("year", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(5, (long) datePartUDF.DATE_PART("month", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(9, (long) datePartUDF.DATE_PART("day", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(11, (long) datePartUDF.DATE_PART("hour", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(49, (long) datePartUDF.DATE_PART("minute", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(45, (long) datePartUDF.DATE_PART("seconds", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(2019, (long) datePartUDF.DATE_PART("year", Date.valueOf("2019-05-09")));
        assertEquals(5, (long) datePartUDF.DATE_PART("month", Date.valueOf("2019-05-09")));
        assertEquals(9, (long) datePartUDF.DATE_PART("day", Date.valueOf("2019-05-09")));
    }
}
