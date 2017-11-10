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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.kyligence.kap.cube.mp.MPSqlCondBuilder;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.DimensionAdvisor;
import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.ModelStatusRequest;
import io.kyligence.kap.source.hive.modelstats.ModelStats;
import io.kyligence.kap.source.hive.modelstats.ModelStatsManager;

@Component("kapModelService")
public class KapModelService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(KapModelService.class);

    public ModelStatusRequest getDiagnoseResult(String modelName) throws IOException {
        ModelStatusRequest modelStatus = extractStatus(modelName);
        return modelStatus;
    }

    public Map<String, DimensionAdvisor.ColumnSuggestionType> inferDimensionSuggestions(String tableName, String prj) {
        return new DimensionAdvisor(getConfig()).inferDimensionSuggestions(tableName, prj);
    }

    private ModelStatusRequest extractStatus(String modelName) throws IOException {
        ModelStats modelStats = ModelStatsManager.getInstance(getConfig()).getModelStats(modelName);
        ModelStatusRequest request = new ModelStatusRequest();
        request.setModelName(modelName);
        List<String> messages = new ArrayList<>();
        int sign = 0;
        if (modelStats.getCounter() == -1) {
            request.setHeathStatus(judgeHealthStatus(-1));
            return request;
        }
        if (!modelStats.isDupliationHealthy()) {
            sign++;
            messages.add(modelStats.getDuplicationResult());
        }
        if (!modelStats.isJointHealthy()) {
            sign++;
            messages.add(modelStats.getJointResult());
        }
        if (!modelStats.isSkewHealthy()) {
            sign++;
            messages.add(modelStats.getSkewResult());
        }
        request.setMessages(messages);
        request.setHeathStatus(judgeHealthStatus(sign));
        return request;
    }

    private ModelStatusRequest.HealthStatus judgeHealthStatus(int sign) {
        ModelStatusRequest.HealthStatus healthStatus;
        switch (sign) {
        case 0:
            healthStatus = ModelStatusRequest.HealthStatus.GOOD;
            break;
        case 1:
            healthStatus = ModelStatusRequest.HealthStatus.WARN;
            break;
        case 2:
            healthStatus = ModelStatusRequest.HealthStatus.BAD;
            break;
        case 3:
            healthStatus = ModelStatusRequest.HealthStatus.TERRIBLE;
            break;
        default:
            healthStatus = ModelStatusRequest.HealthStatus.NONE;
            break;
        }
        return healthStatus;
    }

    public boolean isFactTableStreaming(String modelName) {
        DataModelDesc modelDesc = getDataModelManager().getDataModelDesc(modelName);
        int sourceTypeType = modelDesc.getRootFactTable().getTableDesc().getSourceType();
        return sourceTypeType == ISourceAware.ID_STREAMING;
    }

    public String[] getColumnSamples(String proj, String table, String column) {
        TableExtDesc tableExtDesc = getTableManager().getTableExt(table, proj);

        int index = 0;
        for (TableExtDesc.ColumnStats s : tableExtDesc.getColumnStats()) {
            if (s.getColumnName().equals(column.toUpperCase()))
                break;
            index++;
        }

        if (tableExtDesc.getSampleRows().isEmpty()) {
            return new String[]{};
        }

        return tableExtDesc.getSampleRows().get(index);
    }

    public boolean validatePartitionFormat(String proj, String table, String column, String format) {
        String[] samples = getColumnSamples(proj, table, column);

        boolean ret = false;
        if (samples.length == 0)
            ret = false;

        for (String s : samples) {
            DateFormat formatter = new SimpleDateFormat(format);
            try {
                Date date = formatter.parse(s);
                ret = s.equals(formatter.format(date));
            } catch (Exception e) {
                ret = false;
            }
        }
        return ret;
    }

    public void removeJobIdFromModelStats(String jobId) {
        ModelStatsManager msManager = ModelStatsManager.getInstance(getConfig());
        for (DataModelDesc desc : getDataModelManager().listDataModels()) {
            try {
                ModelStats stats = msManager.getModelStats(desc.getName());
                String statsJobId = stats.getJodID();
                if (statsJobId != null && statsJobId.equals(jobId)) {
                    stats.setJodID(null);
                    msManager.saveModelStats(stats);
                }
            } catch (IOException e) {
                logger.error("Failed to get model stats: {}", desc.getName());
            }
        }
    }

    /**
     * check if the computed column expressions are valid ( in hive)
     */
    public boolean checkCCExpression(final KapModel dataModelDesc, String project) throws IOException {

        dataModelDesc.setDraft(false);
        if (dataModelDesc.getUuid() == null)
            dataModelDesc.updateRandomUuid();

        dataModelDesc.init(getConfig(), getTableManager().getAllTablesMap(project),
                getDataModelManager().listDataModels());

        for (ComputedColumnDesc cc : dataModelDesc.getComputedColumnDescs()) {

            //check by calcite parser
            String ccExpression = KapQueryUtil.massageComputedColumn(cc.getExpression(), project, "");
            cc.simpleParserCheck(ccExpression, dataModelDesc.getAliasMap().keySet());

            //check by hive cli, this could be slow
            StringBuilder sb = new StringBuilder();
            sb.append("select ");
            sb.append(ccExpression);
            sb.append(" ");
            JoinedFlatTable.appendJoinStatement(new IJoinedFlatTableDesc() {
                @Override
                public String getTableName() {
                    return null;
                }

                @Override
                public DataModelDesc getDataModel() {
                    return dataModelDesc;
                }

                @Override
                public List<TblColRef> getAllColumns() {
                    return null;
                }

                @Override
                public int getColumnIndex(TblColRef colRef) {
                    return 0;
                }

                @Override
                public SegmentRange getSegRange() {
                    return null;
                }

                @Override
                public TblColRef getDistributedBy() {
                    return null;
                }

                @Override
                public TblColRef getClusterBy() {
                    return null;
                }

                @Override
                public ISegment getSegment() {
                    return null;
                }
            }, sb, false);
            sb.append(" limit 0");

            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
            hiveCmdBuilder.addStatement(sb.toString());

            long ts = System.currentTimeMillis();
            Pair<Integer, String> response = KylinConfig.getInstanceFromEnv().getCliCommandExecutor()
                    .execute(hiveCmdBuilder.toString());
            logger.debug("Spent " + (System.currentTimeMillis() - ts)
                    + " ms to execute the hive command to validate computed column expression: " + cc.getExpression());
            if (response.getFirst() != 0) {
                throw new IllegalArgumentException("The expression " + cc.getExpression()
                        + " failed syntax check with output message: " + response.getSecond());
            }
        }

        return true;
    }

    public void preProcessBeforeModelSave(KapModel model, String project) {
        String condBldClz = null;
        if (model.isMultiLevelPartitioned()) {
            // set MPHiveCondBuilder if it is a MP model
            condBldClz = MPSqlCondBuilder.class.getName();
        } else {
            // UNSET MPHiveCondBuilder if it is NOT a MP model
            condBldClz = model.getPartitionDesc().getPartitionConditionBuilderClz();
            if (condBldClz.equals(MPSqlCondBuilder.class.getName()))
                condBldClz = null;
        }
        model.getPartitionDesc().setPartitionConditionBuilderClz(condBldClz);

        // (MP cube) check fact table is not streaming
        if (model.isMultiLevelPartitioned()) {
            String rootFactTableName = model.getRootFactTableName();
            TableDesc factTable = getTableManager().getTableDesc(rootFactTableName, project);
            if (factTable.getSourceType() == ISourceAware.ID_STREAMING) {
                KapMessage msg = KapMsgPicker.getMsg();
                throw new BadRequestException(msg.getMPMODEL_HATES_STREAMING());
            }
        }

        // Update CC expression from query transformers
        for (ComputedColumnDesc ccDesc : model.getComputedColumnDescs()) {
            ccDesc.setInnerExpression(KapQueryUtil.massageComputedColumn(ccDesc.getExpression(), project, ""));
        }
    }

}
