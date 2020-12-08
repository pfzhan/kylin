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

package org.apache.kylin.metrics.lib.impl;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;

public class RecordEventTimeDetail {
    private static final TimeZone timeZone;
    private static final ThreadLocal<SimpleDateFormat> dateFormatThreadLocal = new ThreadLocal<SimpleDateFormat>();
    private static final ThreadLocal<SimpleDateFormat> timeFormatThreadLocal = new ThreadLocal<SimpleDateFormat>();

    static {
        timeZone = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone());
    }

    public final String year_begin_date;
    public final String month_begin_date;
    public final String date;
    public final String time;
    public final int hour;
    public final int minute;
    public final int second;
    public final String week_begin_date;

    public RecordEventTimeDetail(long timeStamp) {
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.setTimeInMillis(timeStamp);

        SimpleDateFormat dateFormat = dateFormatThreadLocal.get();
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            dateFormat.setTimeZone(timeZone);
            dateFormatThreadLocal.set(dateFormat);
        }
        SimpleDateFormat timeFormat = timeFormatThreadLocal.get();
        if (timeFormat == null) {
            timeFormat = new SimpleDateFormat("HH:mm:ss");
            timeFormat.setTimeZone(timeZone);
            timeFormatThreadLocal.set(timeFormat);
        }

        String yearStr = String.format("%04d", calendar.get(Calendar.YEAR));
        String monthStr = String.format("%02d", calendar.get(Calendar.MONTH) + 1);
        this.year_begin_date = yearStr + "-01-01";
        this.month_begin_date = yearStr + "-" + monthStr + "-01";
        this.date = dateFormat.format(calendar.getTime());
        this.time = timeFormat.format(calendar.getTime());
        this.hour = calendar.get(Calendar.HOUR_OF_DAY);
        this.minute = calendar.get(Calendar.MINUTE);
        this.second = calendar.get(Calendar.SECOND);

        long timeStampForWeekBegin = timeStamp;
        timeStampForWeekBegin -= 3600000 * 24 * (calendar.get(Calendar.DAY_OF_WEEK) - 1);
        calendar.setTimeInMillis(timeStampForWeekBegin);
        this.week_begin_date = dateFormat.format(calendar.getTime());
    }
}
