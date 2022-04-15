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

import static org.apache.kylin.common.exception.ServerErrorCode.RELOAD_TABLE_FAILED;
import static org.apache.kylin.metadata.datatype.DataType.DECIMAL;
import static org.apache.kylin.metadata.datatype.DataType.DOUBLE;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.schema.ReloadTableContext;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.metadata.streaming.KafkaConfigManager;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.rest.request.StreamingRequest;
import io.kyligence.kap.rest.util.TableUtils;
import lombok.val;

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
        val tableManager = getManager(NTableMetadataManager.class, project);
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

    /**
     * StreamingTable decimal convert to double in StreamingRequest
     */
    @Transaction(project = 0)
    public void decimalConvertToDouble(String project, StreamingRequest streamingRequest) {
        aclEvaluate.checkProjectWritePermission(project);
        Arrays.stream(streamingRequest.getTableDesc().getColumns()).forEach(column -> {
            if (StringUtils.equalsIgnoreCase(DECIMAL, column.getDatatype())) {
                column.setDatatype(DOUBLE);
            }
        });
    }

    public void checkColumns(StreamingRequest streamingRequest) {
        String batchTableName = streamingRequest.getKafkaConfig().getBatchTable();
        String project = streamingRequest.getProject();
        if (!org.apache.commons.lang.StringUtils.isEmpty(batchTableName)) {
            TableDesc batchTableDesc = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getTableDesc(batchTableName);
            if (!checkColumnsMatch(batchTableDesc.getColumns(), streamingRequest.getTableDesc().getColumns())) {
                throw new KylinException(RELOAD_TABLE_FAILED, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getBATCH_STREAM_TABLE_NOT_MATCH(), batchTableName));
            }
            TableUtils.checkTimestampColumn(batchTableDesc);
            streamingRequest.getTableDesc().setColumns(batchTableDesc.getColumns().clone());
        } else {
            TableUtils.checkTimestampColumn(streamingRequest.getTableDesc());
        }
    }

    private boolean checkColumnsMatch(ColumnDesc[] batchColumnDescs, ColumnDesc[] streamColumnDescs) {
        if (batchColumnDescs.length != streamColumnDescs.length) {
            return false;
        }

        List<String> batchColumns = Arrays.stream(batchColumnDescs).map(ColumnDesc::getName).sorted()
                .collect(Collectors.toList());
        List<String> streamColumns = Arrays.stream(streamColumnDescs).map(ColumnDesc::getName).sorted()
                .collect(Collectors.toList());
        if (!batchColumns.equals(streamColumns)) {
            return false;
        }
        return true;
    }

}
