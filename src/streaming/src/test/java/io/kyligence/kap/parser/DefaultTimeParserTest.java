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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DefaultTimeParserTest extends AbstractTimeParserTestBase {
    private static final String TS_TIMEZONE = "tsTimezone";
    private static final String TS_PARSER = "io.kyligence.kap.parser.DefaultTimeParser";

    @Test
    public void testParseBlankTime() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_TIMEZONE, "GMT");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("");
        Assert.assertEquals(0L, time);
        time = timeParser.parseTime(null);
        Assert.assertEquals(0L, time);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseNoValidTime() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_TIMEZONE, "GMT");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        timeParser.parseTime("1a11b23423c");
    }

    @Test
    public void testParseTimeWithTimeZone_GMT() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_TIMEZONE, "GMT");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("1569240794000");
        Assert.assertEquals(1569240794000L, time);
    }

    @Test
    public void testParseTimeWithTimeZone_GMT_0() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_TIMEZONE, "GMT+0");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("1569240794000");
        Assert.assertEquals(1569240794000L, time);
    }

    @Test
    public void testParseTimeWithTimeZone_GMT_8() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_TIMEZONE, "GMT+8");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("1569211994000");
        Assert.assertEquals(1569240794000L, time);
    }
}
