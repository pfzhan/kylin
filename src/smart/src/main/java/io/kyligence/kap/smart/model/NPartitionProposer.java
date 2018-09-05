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

import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NPartitionProposer extends NAbstractModelProposer {
    
    private final static Logger LOGGER = LoggerFactory.getLogger(NPartitionProposer.class);

    public NPartitionProposer(NSmartContext.NModelContext modelContext) {
        super(modelContext);
    }

    @Override
    protected void doPropose(NDataModel modelDesc) {
        KylinConfig config = modelContext.getSmartContext().getKylinConfig();
        String project = modelContext.getSmartContext().getProject();
        NDataLoadingRangeManager dataRangeManager = NDataLoadingRangeManager.getInstance(config, project);

        String rootFactTable = modelDesc.getRootFactTableName();
        NDataLoadingRange range = dataRangeManager.getDataLoadingRange(rootFactTable);
        if (range == null)
            return;

        String partitionColName = range.getColumnName();
        TblColRef partitionCol = modelDesc.findColumn(partitionColName);
        if (partitionCol == null) {
            return;
        }
        if (!partitionCol.getType().isDate()) {
            // Currently only date type supported as partition column
            LOGGER.warn("{} is not date type, cannot be used as partition column", partitionCol);
            return;
        }

        PartitionDesc partition = new PartitionDesc();
        partition.setPartitionDateColumn(partitionCol.getIdentity());
        modelDesc.setPartitionDesc(partition);
    }
}
