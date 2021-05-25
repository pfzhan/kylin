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

package io.kyligence.kap.clickhouse.job;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageConstants;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import lombok.val;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class ClickHouseMerge extends ClickHouseLoad {
    private Set<String> oldSegmentIds;
    private String targetSegmentId;

    public ClickHouseMerge() {
        this.setName(SecondStorageConstants.STEP_MERGE_SECOND_STORAGE);
    }

    @Override
    protected void init() {
        oldSegmentIds = Sets.newHashSet(getParam(NBatchConstants.P_SEGMENT_IDS).split(","));
        targetSegmentId = getParam(SecondStorageConstants.P_MERGED_SEGMENT_ID);
        Preconditions.checkNotNull(targetSegmentId);
    }

    private TableFlow getDataFlow() {
        val tableFlowManager = SecondStorage.tableFlowManager(getConfig(), getProject());
        return tableFlowManager.get(getTargetSubject()).orElse(null);
    }

    @Override
    public Set<String> getSegmentIds() {
        if (oldSegmentIds == null || oldSegmentIds.isEmpty()) {
            return Collections.emptySet();
        }
        val tableFlow = getDataFlow();
        Preconditions.checkNotNull(tableFlow);
        val existedSegments = tableFlow.getTableDataList().stream().flatMap(tableData -> tableData.getPartitions().stream())
                .map(TablePartition::getSegmentId).collect(Collectors.toSet());
        return existedSegments.containsAll(oldSegmentIds) ? Collections.emptySet() : oldSegmentIds;
    }

    @Override
    protected void updateMeta() {
        if (!getSegmentIds().isEmpty()) {
            super.updateMeta();
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            return getDataFlow().update(copied -> copied.getTableDataList().forEach(tableData -> {
                tableData.mergePartitions(oldSegmentIds, targetSegmentId);
            }));
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }
}
