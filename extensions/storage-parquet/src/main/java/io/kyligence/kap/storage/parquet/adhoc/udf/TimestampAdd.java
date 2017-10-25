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
import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class TimestampAdd extends UDF {
    public Timestamp evaluate(String arg1, Integer arg2, Timestamp arg3) {
        if (StringUtils.isAnyEmpty(arg1) && arg3 != null) {
            return null;
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(arg3);

        switch (arg1.toUpperCase()) {
        case "FRAC_SECOND":
        case "SQL_TSI_FRAC_SECOND":
            cal.add(Calendar.MILLISECOND, arg2);
            break;
        case "SECOND":
        case "SQL_TSI_SECOND":
            cal.add(Calendar.SECOND, arg2);
            break;
        case "MINUTE":
        case "SQL_TSI_MINUTE":
            cal.add(Calendar.MINUTE, arg2);
            break;
        case "HOUR":
        case "SQL_TSI_HOUR":
            cal.add(Calendar.HOUR, arg2);
            break;
        case "DAY":
        case "SQL_TSI_DAY":
            cal.add(Calendar.DATE, arg2);
            break;
        case "WEEK":
        case "SQL_TSI_WEEK":
            cal.add(Calendar.WEEK_OF_YEAR, arg2);
            break;
        case "MONTH":
        case "SQL_TSI_MONTH":
            cal.add(Calendar.MONTH, arg2);
            break;
        case "QUARTER":
        case "SQL_TSI_QUARTER":
            cal.add(Calendar.MONTH, arg2 * 3);
            break;
        case "YEAR":
        case "SQL_TSI_YEAR":
            cal.add(Calendar.YEAR, arg2);
            break;
        default:
            return null;
        }

        return new Timestamp(cal.getTimeInMillis());
    }

    public Date evaluate(String arg1, Integer arg2, Date arg3) {
        if (StringUtils.isAnyEmpty(arg1) && arg3 != null) {
            return null;
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(arg3);

        switch (arg1.toUpperCase()) {
        case "FRAC_SECOND":
        case "SQL_TSI_FRAC_SECOND":
            cal.add(Calendar.MILLISECOND, arg2);
            break;
        case "SECOND":
        case "SQL_TSI_SECOND":
            cal.add(Calendar.SECOND, arg2);
            break;
        case "MINUTE":
        case "SQL_TSI_MINUTE":
            cal.add(Calendar.MINUTE, arg2);
            break;
        case "HOUR":
        case "SQL_TSI_HOUR":
            cal.add(Calendar.HOUR, arg2);
            break;
        case "DAY":
        case "SQL_TSI_DAY":
            cal.add(Calendar.DATE, arg2);
            break;
        case "WEEK":
        case "SQL_TSI_WEEK":
            cal.add(Calendar.WEEK_OF_YEAR, arg2);
            break;
        case "MONTH":
        case "SQL_TSI_MONTH":
            cal.add(Calendar.MONTH, arg2);
            break;
        case "QUARTER":
        case "SQL_TSI_QUARTER":
            cal.add(Calendar.MONTH, arg2 * 3);
            break;
        case "YEAR":
        case "SQL_TSI_YEAR":
            cal.add(Calendar.YEAR, arg2);
            break;
        default:
            return null;
        }

        return new Date(cal.getTimeInMillis());
    }
}