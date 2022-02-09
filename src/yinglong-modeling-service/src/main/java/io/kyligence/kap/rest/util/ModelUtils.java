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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARTITION_COLUMN;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.PartitionDesc;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.constant.ModelAttributeEnum;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;

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

    public static void checkPartitionColumn(NDataModel model, PartitionDesc partitionDesc, String errMsg) {
        if (!model.isBroken() && model.isStreaming() && isPartitionEmptyAndNotTimeStamp(partitionDesc)) {
            throw new KylinException(INVALID_PARTITION_COLUMN, errMsg);
        }
    }

    private static boolean isPartitionEmptyAndNotTimeStamp(PartitionDesc partitionDesc) {
        return PartitionDesc.isEmptyPartitionDesc(partitionDesc)
                || !DateFormat.isTimestampFormat(partitionDesc.getPartitionDateFormat());
    }

    public static void checkPartitionColumn(String project, String modelId, String errMsg) {
        val config = KylinConfig.getInstanceFromEnv();

        val modelMgr = NDataModelManager.getInstance(config, project);
        val model = modelMgr.getDataModelDesc(modelId);
        if (!model.isBroken() && model.isStreaming()) {
            val partitionDesc = model.getPartitionDesc();
            checkPartitionColumn(model, partitionDesc, errMsg);
        }
    }

    public static boolean isArgMatch(String valueToMatch, boolean exactMatch, String originValue) {
        return StringUtils.isEmpty(valueToMatch) || (exactMatch && originValue.equalsIgnoreCase(valueToMatch))
                || (!exactMatch
                        && originValue.toLowerCase(Locale.ROOT).contains(valueToMatch.toLowerCase(Locale.ROOT)));

    }

    public static Set<NDataModel> getFilteredModels(String project, List<ModelAttributeEnum> modelAttributes,
                                              List<NDataModel> models) {
        Set<ModelAttributeEnum> modelAttributeSet = Sets
                .newHashSet(modelAttributes == null ? Collections.emptyList() : modelAttributes);
        Set<NDataModel> filteredModels = new HashSet<>();
        if (SecondStorageUtil.isProjectEnable(project)) {
            val secondStorageInfos = SecondStorageUtil.setSecondStorageSizeInfo(models);
            val it = models.listIterator();
            while (it.hasNext()) {
                val secondStorageInfo = secondStorageInfos.get(it.nextIndex());
                NDataModelResponse modelResponse = (NDataModelResponse) it.next();
                modelResponse.setSecondStorageNodes(secondStorageInfo.getSecondStorageNodes());
                modelResponse.setSecondStorageSize(secondStorageInfo.getSecondStorageSize());
                modelResponse.setSecondStorageEnabled(secondStorageInfo.isSecondStorageEnabled());
            }
            if (modelAttributeSet.contains(ModelAttributeEnum.SECOND_STORAGE)) {
                filteredModels.addAll(ModelAttributeEnum.SECOND_STORAGE.filter(models));
                modelAttributeSet.remove(ModelAttributeEnum.SECOND_STORAGE);
            }
        }
        for (val attr : modelAttributeSet) {
            filteredModels.addAll(attr.filter(models));
        }
        return filteredModels;
    }

    public static void addSecondStorageInfo(String project, List<NDataModel> models) {
        if (SecondStorageUtil.isProjectEnable(project)) {
            val secondStorageInfos = SecondStorageUtil.setSecondStorageSizeInfo(models);
            val it = models.listIterator();
            while (it.hasNext()) {
                val secondStorageInfo = secondStorageInfos.get(it.nextIndex());
                NDataModelResponse modelResponse = (NDataModelResponse) it.next();
                modelResponse.setSecondStorageNodes(secondStorageInfo.getSecondStorageNodes());
                modelResponse.setSecondStorageSize(secondStorageInfo.getSecondStorageSize());
                modelResponse.setSecondStorageEnabled(secondStorageInfo.isSecondStorageEnabled());
            }
        }
    }

}
