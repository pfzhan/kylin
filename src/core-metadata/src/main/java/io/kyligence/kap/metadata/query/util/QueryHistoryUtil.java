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

import java.sql.JDBCType;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.JsonUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistorySql;

public class QueryHistoryUtil {

    private QueryHistoryUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static String getDownloadData(QueryHistory queryHistory, ZoneOffset zoneOffset, int zoneOffsetOfHours) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.getDefault(Locale.Category.FORMAT));
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone(zoneOffset));
        String sign = zoneOffsetOfHours > 0 ? "+" : "";
        String formatQueryTime = simpleDateFormat.format(queryHistory.getQueryTime()) + " GMT" + sign
                + zoneOffsetOfHours;

        String answerBy;
        if (queryHistory.getNativeQueryRealizations() != null && !queryHistory.getNativeQueryRealizations().isEmpty()) {
            answerBy = "\"[" + StringUtils.join(queryHistory.getNativeQueryRealizations().stream()
                    .map(NativeQueryRealization::getModelAlias).collect(Collectors.toList()), ',') + "]\"";
        } else {
            answerBy = queryHistory.getEngineType();
        }
        String queryMsg = queryHistory.getQueryHistoryInfo().getQueryMsg();
        if (StringUtils.isNotEmpty(queryMsg)) {
            queryMsg = "\"" + queryMsg.replace("\"", "\"\"") + "\"";
        }

        QueryHistorySql queryHistorySql = queryHistory.getQueryHistorySql();
        String sql = queryHistorySql.getNormalizedSql();

        return StringUtils.join(Lists.newArrayList(formatQueryTime, queryHistory.getDuration() + "ms",
                queryHistory.getQueryId(), "\"" + sql.replace("\"", "\"\"") + "\"", answerBy,
                queryHistory.getQueryStatus(), queryHistory.getHostName(), queryHistory.getQuerySubmitter(), queryMsg),
                ',').replaceAll("\n|\r", " ");
    }

    public static String toQueryHistorySqlText(QueryHistorySql queryHistorySql) throws JsonProcessingException {
        return JsonUtil.writeValueAsString(queryHistorySql);
    }

    public static String toDataType(String className) throws ClassNotFoundException {
        ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(Class.forName(className));
        return JDBCType.valueOf(rep.typeId).getName();
    }
}
