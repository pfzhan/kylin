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

package org.apache.kylin.common.util;

import io.kyligence.kap.junit.TimeZoneTestRunner;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by dongli on 1/4/16.
 */
@RunWith(TimeZoneTestRunner.class)
public class DateFormatTest {

    @Test
    public void testIsSupportedDateFormat() {
        Assert.assertTrue(DateFormat.isSupportedDateFormat("20100101"));
        Assert.assertTrue(DateFormat.isSupportedDateFormat("2010-01-01"));
        Assert.assertTrue(DateFormat.isSupportedDateFormat("2010/01/01"));
        Assert.assertTrue(DateFormat.isSupportedDateFormat("2010.01.01"));

        Assert.assertTrue(DateFormat.isSupportedDateFormat("2010-01-01 01:01:01"));
        Assert.assertTrue(DateFormat.isSupportedDateFormat("2010-01-01 01:00:00.000"));

        Assert.assertFalse(DateFormat.isSupportedDateFormat("2010-1-1"));
        Assert.assertFalse(DateFormat.isSupportedDateFormat("2010/1/1"));
        Assert.assertFalse(DateFormat.isSupportedDateFormat("2010.1.1"));

        Assert.assertFalse(DateFormat.isSupportedDateFormat("2010-1-1 01:01:01"));
        Assert.assertFalse(DateFormat.isSupportedDateFormat("2010-1-1 01:00:00.000"));

        Assert.assertFalse(DateFormat.isSupportedDateFormat("2010-01-01 1:1:1"));
        Assert.assertFalse(DateFormat.isSupportedDateFormat("2010-01-01 01:00:00.1"));

        Assert.assertFalse(DateFormat.isSupportedDateFormat("2010-01"));
        Assert.assertFalse(DateFormat.isSupportedDateFormat("abc"));
    }

    @Test
    public void testGetFormattedDate() {
        String testDate = "2010-01-01";
        String zoneId = ZoneId.systemDefault().getId();

        String correctTime = null;
        switch (zoneId) {
            case "GMT+08:00":
                correctTime = "1262275200000";
                break;
            case "America/Chicago":
                correctTime = "1262325600000";
                break;
            case "America/Los_Angeles":
                correctTime = "1262332800000";
                break;
            case "UTC":
                correctTime = "1262304000000";
                break;
            default:
                Assert.fail("can not find the zoneId: " + zoneId);
        }


        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(DateFormat.DEFAULT_DATE_PATTERN);
        Date date = DateTime.parse(testDate, dateTimeFormatter).
                withZone(DateTimeZone.getDefault()).toLocalDateTime().toDate();

        Assert.assertEquals(correctTime, String.valueOf(date.getTime()));

        Assert.assertEquals(String.valueOf(date.getTime()),
                DateFormat.getFormattedDate("20100101", DateFormat.COMPACT_DATE_PATTERN));

        Assert.assertEquals(String.valueOf(date.getTime()),
                DateFormat.getFormattedDate("2010-01-01", DateFormat.DEFAULT_DATE_PATTERN));

        Assert.assertEquals(String.valueOf(date.getTime()),
                DateFormat.getFormattedDate("2010/01/01", DateFormat.DEFAULT_DATE_PATTERN_WITH_SLASH));

        Assert.assertEquals(String.valueOf(date.getTime()),
                DateFormat.getFormattedDate("2010.01.01", DateFormat.DEFAULT_DATE_PATTERN_WITH_DOT));

        Assert.assertEquals(String.valueOf(date.getTime()),
                DateFormat.getFormattedDate("2010-01-01 00:00:00",
                        DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS));

        Assert.assertEquals(String.valueOf(date.getTime()),
                DateFormat.getFormattedDate("2010-01-01 00:00:00.000",
                        DateFormat.DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS));

        Assert.assertNotEquals(String.valueOf(date.getTime()),
                DateFormat.getFormattedDate("2010-01-01 02:02:02",
                        DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS));

        Assert.assertNotEquals(String.valueOf(date.getTime()),
                DateFormat.getFormattedDate("2010-01-01 02:02:02.333",
                        DateFormat.DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS));

        try {
            Assert.assertEquals(String.valueOf(date.getTime()),
                    DateFormat.getFormattedDate("18:18",
                            DateFormat.DEFAULT_TIME_PATTERN_WITHOUT_SECONDS));
            Assert.fail();
        } catch (DateTimeException e) {

        } catch (Exception e) {
            Assert.fail();
        }

        try {
            Assert.assertEquals(String.valueOf(date.getTime()),
                    DateFormat.getFormattedDate("18:18:18",
                            DateFormat.DEFAULT_TIME_PATTERN));
            Assert.fail();
        } catch (DateTimeException e) {

        } catch (Exception e) {
            Assert.fail();
        }

        try {
            Assert.assertEquals(String.valueOf(date.getTime()),
                    DateFormat.getFormattedDate("18:18:18:888",
                            DateFormat.DEFAULT_TIME_PATTERN_WITH_MILLISECONDS));
            Assert.fail();
        } catch (DateTimeException e) {

        } catch (Exception e) {
            Assert.fail();
        }

        for (String tDate : Arrays.asList(
                "20100101", "2010/01/01", "2010.01.01",
                "18:18:18", "18:18:18.888", "18:18",
                "2010-01-01 ", "2010-01-01 02:02:02", "2010-01-01 02:02:02.333",
                "2010-01-0", "010-01-01", "2010", "2010-01", "-2010-01-01")) {
            try {
                DateFormat.getFormattedDate(tDate, DateFormat.DEFAULT_DATE_PATTERN);
                Assert.fail();
            } catch (DateTimeParseException e) {

            } catch (Exception e) {
                Assert.fail();
            }
        }

        for (String tDate : Arrays.asList(
                "20100101", "2010/01/01", "2010.01.01",
                "18:18:18", "18:18:18.888", "18:18",
                "2010-01-01 00", "2010-01-01 02:02", "2010-01-01 02:02.333",
                "2010-01-0", "010-01-01", "2010", "2010-01", "-2010-01-01")) {
            try {
                DateFormat.getFormattedDate(tDate, DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
                Assert.fail();
            } catch (DateTimeParseException e) {

            } catch (Exception e) {
                Assert.fail();
            }
        }
    }
}
