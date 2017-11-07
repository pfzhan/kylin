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

package io.kyligence.kap.storage.parquet.adhoc.udf;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.Duration;

public class TimestampDiff extends UDF {

    public Long evaluate(String arg1, Timestamp arg2, Timestamp arg3) {
        if (StringUtils.isAnyEmpty(arg1) || arg2 == null || arg3 == null) {
            return null;
        }
        Duration duration = new Duration(arg2.getTime(), arg3.getTime());
        return convertDuration(duration, arg1);
    }

    public Long evaluate(String arg1, Timestamp arg2, Date arg3) {
        if (StringUtils.isAnyEmpty(arg1) || arg2 == null || arg3 == null) {
            return null;
        }
        Duration duration = new Duration(arg2.getTime(), arg3.getTime());
        return convertDuration(duration, arg1);
    }

    public Long evaluate(String arg1, Timestamp arg2, Time arg3) {
        if (StringUtils.isAnyEmpty(arg1) || arg2 == null || arg3 == null) {
            return null;
        }
        Duration duration = new Duration(arg2.getTime(), arg3.getTime());
        return convertDuration(duration, arg1);
    }

    public Long evaluate(String arg1, Date arg2, Timestamp arg3) {
        if (StringUtils.isAnyEmpty(arg1) || arg2 == null || arg3 == null) {
            return null;
        }
        Duration duration = new Duration(arg2.getTime(), arg3.getTime());
        return convertDuration(duration, arg1);
    }

    public Long evaluate(String arg1, Date arg2, Date arg3) {
        if (StringUtils.isAnyEmpty(arg1) || arg2 == null || arg3 == null) {
            return null;
        }
        Duration duration = new Duration(arg2.getTime(), arg3.getTime());
        return convertDuration(duration, arg1);
    }

    public Long evaluate(String arg1, Date arg2, Time arg3) {
        if (StringUtils.isAnyEmpty(arg1) || arg2 == null || arg3 == null) {
            return null;
        }
        Duration duration = new Duration(arg2.getTime(), arg3.getTime());
        return convertDuration(duration, arg1);
    }

    public Long evaluate(String arg1, Time arg2, Timestamp arg3) {
        if (StringUtils.isAnyEmpty(arg1) || arg2 == null || arg3 == null) {
            return null;
        }
        Duration duration = new Duration(arg2.getTime(), arg3.getTime());
        return convertDuration(duration, arg1);
    }

    public Long evaluate(String arg1, Time arg2, Date arg3) {
        if (StringUtils.isAnyEmpty(arg1) || arg2 == null || arg3 == null) {
            return null;
        }
        Duration duration = new Duration(arg2.getTime(), arg3.getTime());
        return convertDuration(duration, arg1);
    }

    public Long evaluate(String arg1, Time arg2, Time arg3) {
        if (StringUtils.isAnyEmpty(arg1) || arg2 == null || arg3 == null) {
            return null;
        }
        Duration duration = new Duration(arg2.getTime(), arg3.getTime());
        return convertDuration(duration, arg1);
    }
    
    private static Long convertDuration(Duration duration, String type) {
        Long durationDays = duration.getStandardDays();
        switch (type.toUpperCase()) {
        case "FRAC_SECOND":
        case "SQL_TSI_FRAC_SECOND":
            return duration.getMillis();
        case "SECOND":
        case "SQL_TSI_SECOND":
            return duration.getStandardSeconds();
        case "MINUTE":
        case "SQL_TSI_MINUTE":
            return duration.getStandardMinutes();
        case "HOUR":
        case "SQL_TSI_HOUR":
            return duration.getStandardHours();
        case "DAY":
        case "SQL_TSI_DAY":
            return durationDays;
        case "WEEK":
        case "SQL_TSI_WEEK":
            return (durationDays / 7);
        case "MONTH":
        case "SQL_TSI_MONTH":
            return (durationDays / 30);
        case "QUARTER":
        case "SQL_TSI_QUARTER":
            return (durationDays / 90);
        case "YEAR":
        case "SQL_TSI_YEAR":
            return (durationDays / 365);
        default:
            return null;
        }
    }
}