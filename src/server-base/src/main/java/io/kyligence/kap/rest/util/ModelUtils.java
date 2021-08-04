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
package io.kyligence.kap.rest.util;

import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TableDesc;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Locale;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARTITION_COLUMN;
import static org.apache.kylin.common.exception.ServerErrorCode.TIMESTAMP_COLUMN_NOT_EXIST;

public class ModelUtils {

    private ModelUtils() {
    }

    public static String computeExpansionRate(long storageBytesSize, long sourceBytesSize) {
        if (storageBytesSize == 0) {
            return "0";
        }
        if (sourceBytesSize == 0) {
            return "-1";
        }
        BigDecimal divide = new BigDecimal(storageBytesSize).divide(new BigDecimal(sourceBytesSize), 4,
                BigDecimal.ROUND_HALF_UP);
        BigDecimal bigDecimal = divide.multiply(new BigDecimal(100)).setScale(2);
        return bigDecimal.toString();
    }

    public static void checkPartitionColumn(String project, String modelId, String errMsg) {
        val config = KylinConfig.getInstanceFromEnv();

        val modelMgr = NDataModelManager.getInstance(config, project);
        val model = modelMgr.getDataModelDesc(modelId);
        if (!model.isBroken() && model.isStreaming()) {
            val partitionDesc = model.getPartitionDesc();
            if (partitionDesc == null || StringUtils.isEmpty(partitionDesc.getPartitionDateColumn())
                    || !DateFormat.isTimestampFormat(partitionDesc.getPartitionDateFormat())) {
                throw new KylinException(INVALID_PARTITION_COLUMN, errMsg);
            }
        }
    }

    public static void checkTimestampColumn(TableDesc tableDesc) {
        val columns = tableDesc.getColumns();
        val result = Arrays.stream(columns).filter(column -> DataType.TIMESTAMP.equals(column.getDatatype())).findAny();
        if (!result.isPresent()) {
            throw new KylinException(TIMESTAMP_COLUMN_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getTIMESTAMP_COLUMN_NOT_EXIST()));
        }
    }
}
