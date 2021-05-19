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

import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 */
public class DateTimeParser extends AbstractTimeParser {

    private static final Logger logger = LoggerFactory.getLogger(DateTimeParser.class);
    private String tsPattern = null;

    private FastDateFormat formatter = null;

    //call by reflection
    public DateTimeParser(Map<String, String> properties) {
        super(properties);
        tsPattern = properties.get(StreamingParser.PROPERTY_TS_PATTERN);

        try {
            formatter = org.apache.kylin.common.util.DateFormat.getDateFormat(tsPattern);
        } catch (Throwable e) {
            throw new IllegalStateException("Invalid tsPattern: '" + tsPattern + "'.");
        }
    }

    /**
     * Parse a string time to a long value (epoch time)
     *
     * @param timeStr
     * @return
     */
    public long parseTime(String timeStr) throws IllegalArgumentException {

        try {
            return formatter.parse(timeStr).getTime();
        } catch (Throwable e) {
            throw new IllegalArgumentException("Invalid value: pattern: '" + tsPattern + "', value: '" + timeStr + "'",
                    e);
        }
    }
}
