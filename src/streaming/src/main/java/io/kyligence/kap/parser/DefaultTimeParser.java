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

import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

/**
 */
public class DefaultTimeParser extends AbstractTimeParser {
    private String tsTimezone = null;

    public DefaultTimeParser(Map<String, String> properties) {
        super(properties);
        tsTimezone = properties.get(StreamingParser.PROPERTY_TS_TIMEZONE);
    }

    /**
     * Parse a string time to a long value (epoch time)
     * @param time
     * @return
     */
    public long parseTime(String time) throws IllegalArgumentException {
        long t;
        if (StringUtils.isEmpty(time)) {
            t = 0;
        } else {
            try {
                ZoneId zoneId = ZoneId.of(tsTimezone);
                TimeZone timeZone = TimeZone.getTimeZone(zoneId);
                Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
                int offsetMilli = calendar.get(Calendar.ZONE_OFFSET);
                t = Long.parseLong(time) + offsetMilli;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return t;
    }
}
