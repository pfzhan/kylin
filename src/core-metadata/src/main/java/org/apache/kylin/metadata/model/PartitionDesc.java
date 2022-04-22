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

package org.apache.kylin.metadata.model;

import static org.apache.kylin.common.util.DateFormat.COMPACT_DATE_PATTERN;
import static org.apache.kylin.common.util.DateFormat.COMPACT_MONTH_PATTERN;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.healthmarketscience.sqlbuilder.BinaryCondition;
import com.healthmarketscience.sqlbuilder.ComboCondition;
import com.healthmarketscience.sqlbuilder.CustomSql;
import com.healthmarketscience.sqlbuilder.NumberValueObject;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.ValueObject;

import io.kyligence.kap.metadata.model.MultiPartitionDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.EqualsAndHashCode;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class PartitionDesc implements Serializable {

    private static String and = " AND ";

    public enum PartitionType implements Serializable {
        APPEND, //
        UPDATE_INSERT // not used since 0.7.1
    }

    @EqualsAndHashCode.Include
    @JsonProperty("partition_date_column")
    private String partitionDateColumn;

    @EqualsAndHashCode.Include
    @JsonProperty("partition_date_start")
    private long partitionDateStart = 0L;//Deprecated

    @EqualsAndHashCode.Include
    @JsonProperty("partition_date_format")
    private String partitionDateFormat;

    @EqualsAndHashCode.Include
    @JsonProperty("partition_type")
    private PartitionType partitionType = PartitionType.APPEND;

    @EqualsAndHashCode.Include
    @JsonProperty("partition_condition_builder")
    private String partitionConditionBuilderClz = DefaultPartitionConditionBuilder.class.getName();

    private TblColRef partitionDateColumnRef;
    private IPartitionConditionBuilder partitionConditionBuilder;

    public void init(NDataModel model) {
        if (StringUtils.isEmpty(partitionDateColumn))
            return;

        partitionDateColumnRef = model.findColumn(partitionDateColumn);
        partitionDateColumn = partitionDateColumnRef.getIdentity();
        partitionConditionBuilder = (IPartitionConditionBuilder) ClassUtil.newInstance(partitionConditionBuilderClz);
    }

    public boolean partitionColumnIsYmdInt() {
        if (partitionDateColumnRef == null)
            return false;

        DataType type = partitionDateColumnRef.getType();
        return (type.isInt() || type.isBigInt()) && DateFormat.isDatePattern(partitionDateFormat);
    }

    public boolean isEmpty() {
        return StringUtils.isEmpty(partitionDateColumn);
    }

    public boolean checkIntTypeDateFormat() {
        DataType type = partitionDateColumnRef.getType();
        if ((type.isInt() || type.isBigInt())) {
            return COMPACT_MONTH_PATTERN.equals(partitionDateFormat)
                    || COMPACT_DATE_PATTERN.equals(partitionDateFormat);
        }
        return true;
    }

    public enum TimestampType implements Serializable {
        MILLISECOND("TIMESTAMP MILLISECOND", 1L, DateFormat.DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS), //
        SECOND("TIMESTAMP SECOND", 1000L, DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);

        public final String name;
        public final long millisecondRatio;
        public final String format;

        TimestampType(String name, long millisecondRatio, String format) {
            this.name = name;
            this.millisecondRatio = millisecondRatio;
            this.format = format;
        }
    }

    public boolean partitionColumnIsTimeMillis() {
        if (partitionDateColumnRef == null)
            return false;

        DataType type = partitionDateColumnRef.getType();
        return type.isBigInt() && !DateFormat.isDatePattern(partitionDateFormat);
    }

    public boolean partitionColumnIsTimestamp() {
        return getTimestampType() != null;
    }

    public TimestampType getTimestampType() {
        for (TimestampType timestampType : TimestampType.values()) {
            if (timestampType.name.equals(partitionDateFormat)) {
                return timestampType;
            }
        }
        return null;
    }

    public static String transformTimestamp2Format(String columnFormat) {
        for (TimestampType timestampType : TimestampType.values()) {
            if (timestampType.name.equals(columnFormat)) {
                return timestampType.format;
            }
        }
        return columnFormat;
    }

    public boolean partitionColumnIsDate() {
        if (partitionDateColumnRef == null)
            return false;

        DataType type = partitionDateColumnRef.getType();
        return type.isDate() && DateFormat.isDatePattern(partitionDateFormat);
    }

    public boolean isPartitioned() {
        return partitionDateColumnRef != null;
    }

    public String getPartitionDateColumn() {
        return partitionDateColumn;
    }

    // for test
    public void setPartitionDateColumn(String partitionDateColumn) {
        this.partitionDateColumn = partitionDateColumn;
    }

    // for test
    public void setPartitionDateColumnRef(TblColRef partitionDateColumnRef) {
        this.partitionDateColumnRef = partitionDateColumnRef;
    }

    @Deprecated
    public long getPartitionDateStart() {
        return partitionDateStart;
    }

    @Deprecated
    public void setPartitionDateStart(long partitionDateStart) {
        this.partitionDateStart = partitionDateStart;
    }

    public String getPartitionDateFormat() {
        return partitionDateFormat;
    }

    public void setPartitionDateFormat(String partitionDateFormat) {
        this.partitionDateFormat = partitionDateFormat;
    }

    public PartitionType getCubePartitionType() {
        return partitionType;
    }

    public void setCubePartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public String getPartitionConditionBuilderClz() {
        return partitionConditionBuilderClz;
    }

    public void setPartitionConditionBuilderClz(String partitionConditionBuilderClz) {
        this.partitionConditionBuilderClz = partitionConditionBuilderClz;
    }

    public IPartitionConditionBuilder getPartitionConditionBuilder() {
        return partitionConditionBuilder;
    }

    public TblColRef getPartitionDateColumnRef() {
        return partitionDateColumnRef;
    }

    public void changeTableAlias(String oldAlias, String newAlias) {
        String table = partitionDateColumn.split("\\.")[0];
        String column = partitionDateColumn.split("\\.")[1];
        if (table.equalsIgnoreCase(oldAlias)) {
            partitionDateColumn = newAlias + "." + column;
        }
    }

    public static boolean isEmptyPartitionDesc(PartitionDesc partitionDesc) {
        return partitionDesc == null || partitionDesc.isEmpty();
    }

    // ============================================================================

    public interface IPartitionConditionBuilder {
        String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange);

        String buildMultiPartitionCondition(final PartitionDesc partDesc, final MultiPartitionDesc multiPartDesc,
                final LinkedList<Long> partitionIds, final ISegment seg, final SegmentRange segRange);
    }

    public static class DefaultPartitionConditionBuilder implements IPartitionConditionBuilder, Serializable {

        public DefaultPartitionConditionBuilder() {
            this.useBigintAsTimestamp = KylinConfig.getInstanceFromEnv().isUseBigIntAsTimestampForPartitionColumn();
        }

        public void setUseBigintAsTimestamp(boolean useBigintAsTimestamp) {
            this.useBigintAsTimestamp = useBigintAsTimestamp;
        }

        boolean useBigintAsTimestamp;

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange) {

            Preconditions.checkState(segRange instanceof SegmentRange.TimePartitionedSegmentRange);
            SegmentRange.TimePartitionedSegmentRange tsr = (SegmentRange.TimePartitionedSegmentRange) segRange;

            long startInclusive = tsr.getStart();
            long endExclusive = tsr.getEnd();

            if (endExclusive <= startInclusive) {
                return "1=1";
            }

            TblColRef partitionDateColumn = partDesc.getPartitionDateColumnRef();
            SqlObject partitionExpression = partitionDateColumn.getSqlObjectExpressionInSourceDB();

            String partitionColumnDateFormat = partDesc.getPartitionDateFormat();

            boolean dataTypeIsIntOrBigInt = false;
            if (partDesc.partitionDateColumnRef != null) {
                DataType type = partDesc.partitionDateColumnRef.getType();
                dataTypeIsIntOrBigInt = (type.isInt() || type.isBigInt());
            }

            Pair<SqlObject, SqlObject> timeRes = buildStartAndEndTime(partDesc, startInclusive, endExclusive,
                    partitionDateColumn, partitionColumnDateFormat, dataTypeIsIntOrBigInt);

            SqlObject startTime = timeRes.getFirst();
            SqlObject endTime = timeRes.getSecond();

            BinaryCondition greaterOrEqualExpression = new BinaryCondition(BinaryCondition.Op.GREATER_THAN_OR_EQUAL_TO,
                    partitionExpression, startTime);
            greaterOrEqualExpression.setDisableParens(true);

            BinaryCondition lessExpression = new BinaryCondition(BinaryCondition.Op.LESS_THAN, partitionExpression,
                    endTime);
            lessExpression.setDisableParens(true);

            ComboCondition c = new ComboCondition(ComboCondition.Op.AND);
            c.setDisableParens(true);
            c.addCondition(greaterOrEqualExpression);
            c.addCondition(lessExpression);

            return c.toString();
        }

        private Pair<SqlObject, SqlObject> buildStartAndEndTime(PartitionDesc partDesc, long startInclusive,
                long endExclusive, TblColRef partitionDateColumn, String partitionColumnDateFormat,
                boolean dataTypeIsIntOrBigInt) {
            SqlObject startTime = null;
            SqlObject endTime = null;
            if (partDesc.partitionColumnIsTimestamp()) {
                TimestampType timestampType = partDesc.getTimestampType();
                startTime = new NumberValueObject(startInclusive / timestampType.millisecondRatio);
                endTime = new NumberValueObject(endExclusive / timestampType.millisecondRatio);
            } else if (partDesc.partitionColumnIsDate()) {
                String time = String.format(Locale.ROOT, "to_date('%s', '%s')",
                        DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat),
                        partitionColumnDateFormat);
                startTime = new CustomSql(time);
                time = String.format(Locale.ROOT, "to_date('%s', '%s')",
                        DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat), partitionColumnDateFormat);
                endTime = new CustomSql(time);
            } else if (dataTypeIsIntOrBigInt) {
                if (!useBigintAsTimestamp) {
                    if (partDesc.partitionDateFormat.equals(COMPACT_MONTH_PATTERN)
                            || partDesc.partitionDateFormat.equals(COMPACT_DATE_PATTERN)) {
                        String time = DateFormat.formatToDateStr(startInclusive, partDesc.partitionDateFormat);
                        startTime = new CustomSql(time);
                        time = DateFormat.formatToDateStr(endExclusive, partDesc.partitionDateFormat);
                        endTime = new CustomSql(time);
                    } else {
                        throw new KylinException(JobErrorCode.JOB_INT_DATE_FORMAT_NOT_MATCH_ERROR,
                                "int/bigint data type only support yyyymm/yyyymmdd format");
                    }
                } else {
                    if (partDesc.partitionColumnIsYmdInt()) {
                        startTime = new NumberValueObject(Integer
                                .valueOf(DateFormat.formatToDateStr(startInclusive, DateFormat.COMPACT_DATE_PATTERN)));
                        endTime = new NumberValueObject(Integer
                                .valueOf(DateFormat.formatToDateStr(endExclusive, DateFormat.COMPACT_DATE_PATTERN)));
                    } else if (partDesc.partitionColumnIsTimeMillis()) {
                        startTime = new NumberValueObject(startInclusive);
                        endTime = new NumberValueObject(endExclusive);
                    }
                }
            } else if (partitionDateColumn != null) {
                Pair<SqlObject, SqlObject> pair = buildSingleColumnRangeCondition(startInclusive, endExclusive,
                        partDesc.getPartitionDateFormat());
                startTime = pair.getFirst();
                endTime = pair.getSecond();
            }
            return Pair.newPair(startTime, endTime);
        }

        @Override
        public String buildMultiPartitionCondition(final PartitionDesc partDesc, final MultiPartitionDesc multiPartDesc,
                final LinkedList<Long> partitionIds, final ISegment seg, final SegmentRange segRange) {
            return "";
        }

        private static Pair<SqlObject, SqlObject> buildSingleColumnRangeCondition(long startInclusive,
                long endExclusive, String partitionColumnDateFormat) {

            String startInc = null;
            String endInc = null;
            if (StringUtils.isBlank(partitionColumnDateFormat)) {
                startInc = String.valueOf(startInclusive);
                endInc = String.valueOf(endExclusive);
            } else {
                startInc = DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat);
                endInc = DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat);
            }

            return Pair.newPair(new ValueObject(startInc), new ValueObject(endInc));
        }
    }

    public static PartitionDesc getCopyOf(PartitionDesc orig) {
        PartitionDesc ret = new PartitionDesc();
        ret.partitionDateColumn = orig.partitionDateColumn;
        ret.partitionDateStart = orig.partitionDateStart; //Deprecated
        ret.partitionDateFormat = orig.partitionDateFormat;
        ret.partitionType = orig.partitionType;
        ret.partitionConditionBuilderClz = orig.partitionConditionBuilderClz;
        ret.partitionConditionBuilder = orig.partitionConditionBuilder;
        return ret;
    }

}
