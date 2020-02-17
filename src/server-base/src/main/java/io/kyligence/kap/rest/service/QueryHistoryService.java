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

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.metadata.query.QueryHistoryRequest;
import io.kyligence.kap.metadata.query.QueryStatistics;
import io.kyligence.kap.rest.response.QueryStatisticsResponse;
import lombok.val;

@Component("queryHistoryService")
public class QueryHistoryService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryService.class);
    public static final String DELETED_MODEL = "Deleted Model";

    @Autowired
    private AclEvaluate aclEvaluate;

    public Map<String, Object> getQueryHistories(QueryHistoryRequest request, final int limit, final int offset) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(request.getProject()));
        aclEvaluate.checkProjectReadPermission(request.getProject());
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao(request.getProject());
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        val modelAliasMap = dataflowManager.listUnderliningDataModels().stream()
                .collect(Collectors.toMap(NDataModel::getAlias, RootPersistentEntity::getUuid));

        HashMap<String, Object> data = new HashMap<>();
        List<QueryHistory> queryHistories = Lists.newArrayList();

        if (request.getSql() == null) {
            request.setSql("");
        }

        if (request.getSql() != null) {
            request.setSql(request.getSql().trim());
        }

        if (haveSpaces(request.getSql())) {
            data.put("query_histories", queryHistories);
            data.put("size", 0);
            return data;
        }

        queryHistoryDAO.getQueryHistoriesByConditions(request, limit, offset).stream().forEach(query -> {
            if (StringUtils.isEmpty(query.getQueryRealizations())) {
                queryHistories.add(query);
                return;
            }

            List<NativeQueryRealization> realizations = Lists.newArrayList();
            query.transformRealizations().forEach(realization -> {
                if (modelAliasMap.containsValue(realization.getModelId())) {
                    String alias = dataModelManager.getDataModelDesc(realization.getModelId()).getAlias();
                    realization.setModelAlias(alias);
                    realizations.add(realization);
                } else {
                    realization.setValid(false);
                    val brokenModel = dataModelManager.getDataModelDesc(realization.getModelId());
                    if (brokenModel == null) {
                        realization.setModelAlias(DELETED_MODEL);
                        realizations.add(realization);
                        return;
                    }
                    if (brokenModel.isBroken()) {
                        realization.setModelAlias(String.format("%s broken", brokenModel.getAlias()));
                        realizations.add(realization);
                    }
                }
            });
            query.setNativeQueryRealizations(realizations);
            queryHistories.add(query);
        });

        data.put("query_histories", queryHistories);
        data.put("size", queryHistoryDAO.getQueryHistoriesSize(request));

        return data;
    }

    private boolean haveSpaces(String text) {
        if (text == null) {
            return false;
        }
        String regex = "[\r|\n|\\s]+";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);
        return matcher.find();
    }

    public QueryStatisticsResponse getQueryStatistics(String project, long startTime, long endTime) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao(project);

        QueryStatistics queryStatistics = queryHistoryDAO.getQueryCountAndAvgDuration(startTime, endTime);
        return new QueryStatisticsResponse(queryStatistics.getCount(), queryStatistics.getMeanDuration());
    }

    public Map<String, Object> getQueryCount(String project, long startTime, long endTime, String dimension) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao(project);
        List<QueryStatistics> queryStatistics;

        if (dimension.equals("model")) {
            queryStatistics = queryHistoryDAO.getQueryCountByModel(startTime, endTime);
            return transformQueryStatisticsByModel(project, queryStatistics, "count");
        }

        queryStatistics = queryHistoryDAO.getQueryCountByTime(startTime, endTime, dimension);
        return transformQueryStatisticsByTime(queryStatistics, "count", dimension);
    }

    public Map<String, Object> getAvgDuration(String project, long startTime, long endTime, String dimension) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao(project);
        List<QueryStatistics> queryStatistics;

        if (dimension.equals("model")) {
            queryStatistics = queryHistoryDAO.getAvgDurationByModel(startTime, endTime);
            return transformQueryStatisticsByModel(project, queryStatistics, "meanDuration");
        }

        queryStatistics = queryHistoryDAO.getAvgDurationByTime(startTime, endTime, dimension);
        return transformQueryStatisticsByTime(queryStatistics, "meanDuration", dimension);
    }

    private Map<String, Object> transformQueryStatisticsByModel(String project, List<QueryStatistics> statistics,
            String fieldName) {
        Map<String, Object> result = Maps.newHashMap();
        NDataModelManager modelManager = getDataModelManager(project);

        statistics.forEach(singleStatistics -> {
            NDataModel model = modelManager.getDataModelDesc(singleStatistics.getModel());
            if (model == null)
                return;
            result.put(model.getAlias(), getValueByField(singleStatistics, fieldName));
        });

        return result;
    }

    private Object getValueByField(QueryStatistics statistics, String fieldName) {
        Object object = null;
        try {
            Field field = statistics.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            object = field.get(statistics);
        } catch (Exception e) {
            logger.error("Error caught when get value from query statistics {}", e.getMessage());
        }

        return object;
    }

    private Map<String, Object> transformQueryStatisticsByTime(List<QueryStatistics> statistics, String fieldName,
            String dimension) {
        Map<String, Object> result = Maps.newHashMap();

        statistics.forEach(singleStatistics -> {
            if (dimension.equals("month")) {
                result.put(singleStatistics.getMonth(), getValueByField(singleStatistics, fieldName));
                return;
            }
            long time = singleStatistics.getTime().toEpochMilli();
            Date date = new Date(time);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            result.put(sdf.format(date), getValueByField(singleStatistics, fieldName));
        });

        return result;
    }

    public Map<String, String> getQueryHistoryTableMap(List<String> projects) {
        if (projects == null) {
            projects = getProjectManager().listAllProjects().stream().map(ProjectInstance::getName)
                    .collect(Collectors.toList());
        }

        Map<String, String> result = Maps.newHashMap();
        for (String project : projects) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(project));
            ProjectInstance projectInstance = getProjectManager().getProject(project);
            if (projectInstance == null)
                throw new BadRequestException(String.format(MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
            result.put(project, getQueryHistoryDao(project).getQueryMetricMeasurement());
        }

        return result;
    }

    public void cleanQueryHistories() {
        String oldTheadName = Thread.currentThread().getName();

        try {
            Thread.currentThread().setName("QueryHistoryCleanWorker");
            val config = KylinConfig.getInstanceFromEnv();
            val projectManager = NProjectManager.getInstance(config);
            for (ProjectInstance project : projectManager.listAllProjects()) {
                try {
                    long startTime = System.currentTimeMillis();
                    logger.info("Start to delete query histories that are beyond max size for project<{}>",
                            project.getName());
                    getQueryHistoryDao(project.getName()).deleteQueryHistoriesIfMaxSizeReached();
                    logger.info("Query histories cleanup for project<{}> finished, it took {}ms", project.getName(),
                            System.currentTimeMillis() - startTime);
                } catch (Exception e) {
                    logger.error("clean query histories<" + project.getName() + "> failed", e);
                }
            }

        } finally {
            Thread.currentThread().setName(oldTheadName);
        }

    }
}
