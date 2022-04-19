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
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageConstants;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import lombok.val;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.handler.SecondStorageIndexCleanJobHandler;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.kyligence.kap.secondstorage.SecondStorageConstants.P_OLD_SEGMENT_IDS;

@NotThreadSafe
public class ClickHouseRefresh extends ClickHouseLoad {
    private Map<String, String> newSegToOld = null;
    private Set<String> oldSegmentIds;

    public ClickHouseRefresh() {
        this.setName(SecondStorageConstants.STEP_REFRESH_SECOND_STORAGE);
    }

    public ClickHouseRefresh(Object notSetId) {
        super(notSetId);
    }

    private void initSegMap() {
        newSegToOld = new HashMap<>();

        String[] segmentIds = getParam(NBatchConstants.P_SEGMENT_IDS).split(",");
        String[] oldSegmentIdsArray = getParam(P_OLD_SEGMENT_IDS).split(",");

        oldSegmentIds = new HashSet<>(Arrays.asList(oldSegmentIdsArray));

        for (int i = 0; i < segmentIds.length; i++) {
            newSegToOld.put(segmentIds[i], oldSegmentIdsArray[i]);
        }
    }

    @Override
    protected List<LoadInfo> preprocessLoadInfo(List<LoadInfo> infoList) {
        if (newSegToOld == null) {
            initSegMap();
        }
        infoList.forEach(info -> info.setOldSegmentId(newSegToOld.get(info.getSegmentId())));
        return infoList;
    }

    @Override
    protected void updateMeta() {
        super.updateMeta();

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getTableFlow().update(copied -> copied.all().forEach(tableData ->
                    tableData.removePartitions(p -> oldSegmentIds.contains(p.getSegmentId()))
            ));

            Set<Long> needDeleteLayoutIds = getTableFlow().getTableDataList().stream()
                    .filter(tableData -> tableData.getPartitions().isEmpty())
                    .map(TableData::getLayoutID).collect(Collectors.toSet());

            getTableFlow().update(copied -> copied.cleanTableData(tableData -> tableData.getPartitions().isEmpty()));
            getTablePlan().update(t -> t.cleanTable(needDeleteLayoutIds));

            if (!needDeleteLayoutIds.isEmpty()) {
                val jobHandler = new SecondStorageIndexCleanJobHandler();
                final JobParam param = SecondStorageJobParamUtil.layoutCleanParam(project, getTargetSubject(), getSubmitter(), needDeleteLayoutIds, Collections.emptySet());
                JobManager.getInstance(getConfig(), project).addJob(param, jobHandler);
            }

            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    private TableFlow getTableFlow() {
        val tableFlowManager = SecondStorage.tableFlowManager(getConfig(), getProject());
        return tableFlowManager.get(getTargetSubject()).orElse(null);
    }

    private TablePlan getTablePlan() {
        val tablePlanManager = SecondStorageUtil.tablePlanManager(getConfig(), project);
        Preconditions.checkState(tablePlanManager.isPresent());
        return tablePlanManager.get().get(getTargetSubject()).orElse(null);
    }
}
