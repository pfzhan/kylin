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

package io.kyligence.kap.rest.service;

import java.util.List;
import java.util.Set;

import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.job.NTableSamplingJob;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.transaction.Transaction;

@Component("tableSamplingService")
public class TableSamplingService extends BasicService {

    @Transaction(project = 1)
    public void sampling(Set<String> tables, String project, int rows) {
        final Message msg = MsgPicker.getMsg();

        List<String> processingIdentity = Lists.newArrayList();
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getConfig(), project);
        NExecutableManager execMgr = NExecutableManager.getInstance(getConfig(), project);
        tables.forEach(table -> {
            if (hasAnotherSamplingJob(project, table)) {
                processingIdentity.add(table);
            } else {
                final TableDesc tableDesc = tableMgr.getTableDesc(table.toUpperCase());
                ExecutablePO po = NExecutableManager
                        .toPO(NTableSamplingJob.create(tableDesc, project, getUsername(), rows), project);
                execMgr.addJob(po);
            }
        });

        if (!processingIdentity.isEmpty()) {
            throw new IllegalStateException(String.format(msg.getFAILED_FOR_IN_SAMPLING(), processingIdentity));
        }
    }

    private boolean hasAnotherSamplingJob(String project, String tableIdentity) {
        NExecutableManager execMgr = NExecutableManager.getInstance(getConfig(), project);
        final List<AbstractExecutable> allExecutables = execMgr.getAllExecutables();
        for (AbstractExecutable executable : allExecutables) {
            if (executable instanceof NTableSamplingJob) {
                NTableSamplingJob job = (NTableSamplingJob) executable;
                if (job.getTableIdentity().equalsIgnoreCase(tableIdentity) && !job.getStatus().isFinalState()) {
                    return true;
                }
            }
        }
        return false;
    }
}
