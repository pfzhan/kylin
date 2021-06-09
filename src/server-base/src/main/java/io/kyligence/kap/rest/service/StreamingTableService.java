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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.model.schema.ReloadTableContext;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.metadata.streaming.KafkaConfigManager;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

@Component("streamingTableService")
public class StreamingTableService extends TableService {

    @Autowired
    private AclEvaluate aclEvaluate;

    @Transaction(project = 0, retry = 1)
    public void reloadTable(String project, TableDesc tableDesc, TableExtDesc extDesc) {
        aclEvaluate.checkProjectWritePermission(project);
        innerReloadTable(project, tableDesc, extDesc);
    }

    @Transaction(project = 0)
    List<String> innerReloadTable(String project, TableDesc tableDesc, TableExtDesc extDesc) {
        val tableManager = getTableManager(project);
        String tableIdentity = tableDesc.getIdentity();
        val originTable = tableManager.getTableDesc(tableIdentity);
        Preconditions.checkNotNull(originTable,
                String.format(Locale.ROOT, MsgPicker.getMsg().getTABLE_NOT_FOUND(), tableIdentity));
        List<String> jobs = Lists.newArrayList();
        val context = new ReloadTableContext();
        context.setTableDesc(tableDesc);
        context.setTableExtDesc(extDesc);

        mergeTable(project, context, false);
        return jobs;
    }

    @Transaction(project = 0)
    public void createKafkaConfig(String project, KafkaConfig kafkaConfig) {
        aclEvaluate.checkProjectWritePermission(project);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        KafkaConfigManager.getInstance(kylinConfig, project).createKafkaConfig(kafkaConfig);
    }

    @Transaction(project = 0)
    public void updateKafkaConfig(String project, KafkaConfig kafkaConfig) {
        aclEvaluate.checkProjectWritePermission(project);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        KafkaConfigManager.getInstance(kylinConfig, project).updateKafkaConfig(kafkaConfig);
    }

}