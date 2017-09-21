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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.model.DimensionAdvisor;
import io.kyligence.kap.rest.request.ModelStatusRequest;
import io.kyligence.kap.source.hive.modelstats.CollectModelStatsJob;
import io.kyligence.kap.source.hive.modelstats.ModelStats;
import io.kyligence.kap.source.hive.modelstats.ModelStatsManager;
import io.kyligence.kap.source.hive.tablestats.HiveTableExtSampleJob;

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
        DataModelDesc modelDesc = getMetadataManager().getDataModelDesc(modelName);
        int sourceTypeType = modelDesc.getRootFactTable().getTableDesc().getSourceType();
        return sourceTypeType == ISourceAware.ID_STREAMING;
    }

    public String[] getColumnSamples(String proj, String table, String column) {
        TableExtDesc tableExtDesc = getMetadataManager().getTableExt(table, proj);

        int index = 0;
        for (TableExtDesc.ColumnStats s : tableExtDesc.getColumnStats()) {
            if (s.getColumnName().equals(column.toUpperCase()))
                break;
            index++;
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

    public Map<String, String> doModelCheck(String project, String modelName, String submitter, SegmentRange tsRange,
            int frequency, int checkList, boolean forceUpdate) throws IOException {

        DataModelDesc dataModelDesc = getMetadataManager().getDataModelDesc(modelName);
        Map<String, String> ret = new HashMap<>();
        String jobId;

        if (ModelCheckList.isSelected(ModelCheckList.doFactStats, checkList)) {
            String factTableName = dataModelDesc.getRootFactTable().getTableIdentity();
            if (hasRunningJob(project, factTableName)) {
                logger.info("Fact table: {}, already has a stats running job.", factTableName);
            } else if (forceUpdate || hasTableStatsJob(project, factTableName) == false) {
                jobId = new HiveTableExtSampleJob(project, factTableName, frequency).start();
                ret.put("Fact table: " + factTableName + " stats job", jobId);
            }
        }

        if (ModelCheckList.isSelected(ModelCheckList.doLookupStats, checkList)) {
            for (JoinTableDesc fTable : dataModelDesc.getJoinTables()) {
                String t = fTable.getTable();
                if (hasRunningJob(project, t)) {
                    logger.info("Lookup table: {}, already has a stats running job.", t);
                    continue;
                }

                if (forceUpdate || hasTableStatsJob(project, t) == false) {
                    jobId = new HiveTableExtSampleJob(project, t, frequency).start();
                    ret.put("Lookup table: " + t + " stats job", jobId);
                }
            }
        }

        if (ModelCheckList.isSelected(ModelCheckList.doModelCheck, checkList)) {
            jobId = new CollectModelStatsJob(project, modelName, submitter, tsRange, frequency).start();
            ret.put("Model: " + modelName + " stats job", jobId);
        }
        return ret;
    }

    private boolean hasRunningJob(String project, String tableName) {
        return (new HiveTableExtSampleJob(project, tableName)).findRunningJob() != null;
    }

    private boolean hasTableStatsJob(String project, String tableName) {
        return getMetadataManager().getTableExt(tableName, project).getColumnStats().size() > 0;
    }
}

class ModelCheckList {
    public final static int doFactStats = 1;
    public final static int doLookupStats = 2;
    public final static int doModelCheck = 4;

    public static boolean isSelected(final int item, final int list) {
        return (list & item) != 0;
    }
}
