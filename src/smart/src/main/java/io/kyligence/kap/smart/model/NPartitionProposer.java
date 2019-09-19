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

package io.kyligence.kap.smart.model;

import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NPartitionProposer extends NAbstractModelProposer {

    NPartitionProposer(NSmartContext.NModelContext modelContext) {
        super(modelContext);
    }

    @Override
    protected void doPropose(NDataModel dataModel) {
        NDataLoadingRangeManager dataRangeManager = NDataLoadingRangeManager.getInstance(kylinConfig, project);

        String rootFactTable = dataModel.getRootFactTableName();
        NDataLoadingRange range = dataRangeManager.getDataLoadingRange(rootFactTable);
        if (range == null)
            return;

        String partitionColName = range.getColumnName();
        TblColRef partitionCol = dataModel.findColumn(partitionColName);
        if (partitionCol == null) {
            return;
        }
        if (!partitionCol.getType().isLegalPartitionColumnType()) {
            log.warn("{} is not legal date type[String, date, timestamp or int], cannot be used as partition column",
                    partitionCol);
            return;
        }

        PartitionDesc partition = new PartitionDesc();
        partition.setPartitionDateColumn(partitionCol.getIdentity());
        partition.setPartitionDateFormat(range.getPartitionDateFormat());
        dataModel.setPartitionDesc(partition);
    }
}
