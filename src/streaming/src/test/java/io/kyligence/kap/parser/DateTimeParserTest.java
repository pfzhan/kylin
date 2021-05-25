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
package io.kyligence.kap.parser;

import org.apache.kylin.common.util.DateFormat;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DateTimeParserTest extends AbstractTimeParserTestBase{
    private static final String TS_PATTERN = "tsPattern";
    private static final String TS_PARSER = "io.kyligence.kap.parser.DateTimeParser";

    @Test
    public void testParse_COMPACT_DATE_PATTERN() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, DateFormat.COMPACT_DATE_PATTERN);
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("20190923");
        Assert.assertEquals(1569168000000L, time);
    }

    @Test
    public void testParse_DEFAULT_DATE_PATTERN() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, DateFormat.DEFAULT_DATE_PATTERN);
        props.put(TS_PATTERN, DateFormat.DEFAULT_DATE_PATTERN);
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("2019-09-23");
        Assert.assertEquals(1569168000000L, time);
    }

    @Test
    public void testParse_DEFAULT_TIME_PATTERN() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, DateFormat.DEFAULT_TIME_PATTERN);
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("12:13:14");
        Assert.assertEquals(15194000L, time);
    }

    @Test
    public void testParse_DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("2019-09-23 12:13:14");
        Assert.assertEquals(1569211994000L, time);
    }

    @Test
    public void testParse_DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, DateFormat.DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS);
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("2019-09-23 12:13:14.135");
        Assert.assertEquals(1569211994135L, time);
    }

    @Test
    public void testParse_SelfDefined_1() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, "MM-dd-yyyy HH:mm:ss");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("09-23-2019 12:13:14");
        Assert.assertEquals(1569211994000L, time);
    }

    @Test
    public void testParse_SelfDefined_2() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, "HH:mm:ss MM-dd-yyyy");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("12:13:14 09-23-2019");
        Assert.assertEquals(1569211994000L, time);
    }

    @Test
    public void testParseTime() {
        Map<String, String> props1 = new HashMap<>();
        props1.put("tsPattern", "yyyy/MM/dd");
        Map<String, String> props2 = new HashMap<>();
        props2.put("tsPattern", "MM.dd.yyyy");
        DateTimeParser parser1 = new DateTimeParser(props1);
        DateTimeParser parser2 = new DateTimeParser(props2);

        Long parsedTime1 = parser1.parseTime("2000/12/1");
        Long parsedTime2 = parser2.parseTime("12.1.2000");

        Assert.assertEquals(parsedTime1, parsedTime2);
    }
}
