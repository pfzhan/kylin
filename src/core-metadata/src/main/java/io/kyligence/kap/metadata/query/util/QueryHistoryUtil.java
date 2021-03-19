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

package io.kyligence.kap.metadata.query.util;

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.query.QueryHistory;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.TimeZone;

public class QueryHistoryUtil {

    public static String getDownloadData(QueryHistory queryHistory, ZoneOffset zoneOffset, int zoneOffsetOfHours) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault(Locale.Category.FORMAT));
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone(zoneOffset));
        String sign = zoneOffsetOfHours > 0 ? "+" : "";
        String formatQueryTime = simpleDateFormat.format(queryHistory.getQueryTime()) + " GMT" + sign + zoneOffsetOfHours;
        return StringUtils.join(Lists.newArrayList(formatQueryTime, queryHistory.getDuration() + "ms", queryHistory.getQueryId(),
                "\"" + queryHistory.getSql().replaceAll("\"", "\"\"") + "\"", queryHistory.getEngineType(), queryHistory.getQueryStatus(),
                queryHistory.getHostName(), queryHistory.getQuerySubmitter()), ',').replaceAll("\n|\r", " ");
    }
}