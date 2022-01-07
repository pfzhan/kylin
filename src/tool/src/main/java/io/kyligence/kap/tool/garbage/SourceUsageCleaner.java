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

package io.kyligence.kap.tool.garbage;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord;

public class SourceUsageCleaner {

    public void cleanup() {

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        long expirationTime = config.getSourceUsageSurvivalTimeThreshold();

        SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(config);
        List<SourceUsageRecord> allRecords = sourceUsageManager.getAllRecordsWithoutInit();
        int totalSize = allRecords.size();
        if (totalSize <= 1)
            return;
        List<SourceUsageRecord> collect = allRecords.stream().filter(
                sourceUsageRecord -> (System.currentTimeMillis() - sourceUsageRecord.getCreateTime()) >= expirationTime)
                .sorted(Comparator.comparingLong(SourceUsageRecord::getCreateTime)).collect(Collectors.toList());
        if (collect.size() == totalSize) {
            collect.remove(totalSize - 1);
        }
        for (SourceUsageRecord record : collect) {
            sourceUsageManager.delSourceUsage(record.resourceName());
        }
    }
}
