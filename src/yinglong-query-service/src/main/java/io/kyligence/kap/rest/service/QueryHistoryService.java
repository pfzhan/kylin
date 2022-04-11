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

import static io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO.fillZeroForQueryStatistics;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffset;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffsetManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.QueryHistoryRequest;
import io.kyligence.kap.metadata.query.QueryStatistics;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.QueryStatisticsResponse;
import lombok.val;

@Component("queryHistoryService")
public class QueryHistoryService extends BasicService implements AsyncTaskQueryHistorySupporter {
    private static final Logger logger = LoggerFactory.getLogger("query");
//    public static final String DELETED_MODEL = "Deleted Model";
//    public static final byte[] CSV_UTF8_BOM = new byte[]{(byte)0xEF, (byte)0xBB, (byte)0xBF};

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("asyncTaskService")
    private AsyncTaskServiceSupporter asyncTaskService;

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    public static final String WEEK = "week";
    public static final String DAY = "day";

    public void downloadQueryHistories(QueryHistoryRequest request, HttpServletResponse response, ZoneOffset zoneOffset,
            Integer timeZoneOffsetHour, boolean onlySql) throws Exception {
        processRequestParams(request);
        if (haveSpaces(request.getSql())) {
            return;
        }
        splitModels(request);

        Future<Long> future = asyncTaskService.runDownloadQueryHistory(request, response, zoneOffset,
                timeZoneOffsetHour, getQueryHistoryDao(), onlySql);
        Long timeCost = future.get(KylinConfig.getInstanceFromEnv().getQueryHistoryDownloadTimeoutSeconds(),
                TimeUnit.SECONDS);
        logger.info("download query history cost {}s", timeCost);
    }

    public Map<String, Object> getQueryHistories(QueryHistoryRequest request, final int limit, final int page) {
        processRequestParams(request);

        HashMap<String, Object> data = new HashMap<>();
        List<QueryHistory> queryHistories = Lists.newArrayList();

        if (haveSpaces(request.getSql())) {
            data.put("query_histories", queryHistories);
            data.put("size", 0);
            return data;
        }

        splitModels(request);

        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        queryHistories = queryHistoryDAO.getQueryHistoriesByConditions(request, limit, page);

        queryHistories.forEach(query -> {
            QueryHistoryInfo queryHistoryInfo = query.getQueryHistoryInfo();
            if ((queryHistoryInfo == null || queryHistoryInfo.getRealizationMetrics() == null
                    || queryHistoryInfo.getRealizationMetrics().isEmpty())
                    && StringUtils.isEmpty(query.getQueryRealizations())) {
                return;
            }
            query.setNativeQueryRealizations(parseQueryRealizationInfo(query, request.getProject()));
        });

        data.put("query_histories", queryHistories);
        data.put("size", queryHistoryDAO.getQueryHistoriesSize(request, request.getProject()));
        return data;
    }

    private void processRequestParams(QueryHistoryRequest request) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(request.getProject()));
        aclEvaluate.checkProjectReadPermission(request.getProject());

        request.setUsername(SecurityContextHolder.getContext().getAuthentication().getName());
        if (aclEvaluate.hasProjectAdminPermission(
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(request.getProject()))) {
            request.setAdmin(true);
        }

        if (request.getSql() == null) {
            request.setSql("");
        }

        if (request.getSql() != null) {
            request.setSql(request.getSql().trim());
        }
    }

    private List<NativeQueryRealization> parseQueryRealizationInfo(QueryHistory query, String project) {
        val noBrokenModels = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .listUnderliningDataModels().stream().collect(Collectors.toMap(NDataModel::getAlias, RootPersistentEntity::getUuid));

        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        List<NativeQueryRealization> realizations = query.transformRealizations();

        realizations.forEach(realization -> {
            NDataModel nDataModel = dataModelManager.getDataModelDesc(realization.getModelId());
            if (noBrokenModels.containsValue(realization.getModelId())) {
                NDataModelResponse model = (NDataModelResponse) modelService
                        .updateReponseAcl(new NDataModelResponse(nDataModel), project);
                realization.setModelAlias(model.getFusionModelAlias());
                realization.setAclParams(model.getAclParams());
                realization.setLayoutExist(
                        isLayoutExist(indexPlanManager, realization.getModelId(), realization.getLayoutId()));

            } else {
                val modelAlias = nDataModel == null ? DELETED_MODEL
                        : String.format(Locale.ROOT, "%s broken", nDataModel.getAlias());
                realization.setModelAlias(modelAlias);
                realization.setValid(false);
                realization.setLayoutExist(false);
            }
        });
        return realizations;
    }

    private boolean isLayoutExist(NIndexPlanManager indexPlanManager, String modelId, Long layoutId) {
        if (layoutId == null)
            return false;
        return indexPlanManager.getIndexPlan(modelId).getLayoutEntity(layoutId) != null;
    }

    private void splitModels(QueryHistoryRequest request) {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        val modelAliasMap = dataflowManager.listUnderliningDataModels().stream()
                .collect(Collectors.toMap(NDataModel::getAlias, RootPersistentEntity::getUuid));
        List<String> realizations = request.getRealizations();
        if (realizations != null && !realizations.isEmpty() && !realizations.contains("modelName")) {
            List<String> modelNames = Lists.newArrayList(realizations);
            modelNames.remove(QueryHistory.EngineType.HIVE.name());
            modelNames.remove(QueryHistory.EngineType.CONSTANTS.name());
            modelNames.remove(QueryHistory.EngineType.RDBMS.name());

            request.setFilterModelIds(modelNames.stream().filter(modelAliasMap::containsKey).map(modelAliasMap::get)
                    .collect(Collectors.toList()));
        }
    }

    public List<String> getQueryHistoryUsernames(QueryHistoryRequest request, int size) {
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        request.setUsername(SecurityContextHolder.getContext().getAuthentication().getName());
        if (aclEvaluate.hasProjectAdminPermission(
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(request.getProject()))) {
            request.setAdmin(true);
        } else {
            throw new ForbiddenException(MsgPicker.getMsg().getEXPORT_RESULT_NOT_ALLOWED());
        }
        List<QueryHistory> queryHistories = queryHistoryDAO.getQueryHistoriesSubmitters(request, size);
        return queryHistories.stream().map(QueryHistory::getQuerySubmitter).collect(Collectors.toList());
    }

    public List<String> getQueryHistoryModels(QueryHistoryRequest request, int size) {
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        request.setUsername(SecurityContextHolder.getContext().getAuthentication().getName());
        if (aclEvaluate.hasProjectAdminPermission(
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(request.getProject()))) {
            request.setAdmin(true);
        }
        List<QueryStatistics> queryStatistics = queryHistoryDAO.getQueryHistoriesModelIds(request, size);

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        val modelAliasMap = dataflowManager.listUnderliningDataModels().stream()
                .collect(Collectors.toMap(RootPersistentEntity::getUuid, NDataModel::getAlias));

        return queryStatistics.stream().map(query -> {
            // engineType && modelId are both saved into queryStatistics
            if (!StringUtils.isEmpty(query.getEngineType())) {
                return query.getEngineType();
            } else if (!StringUtils.isEmpty(query.getModel()) && modelAliasMap.containsKey(query.getModel())) {
                return modelAliasMap.get(query.getModel());
            } else {
                return null;
            }
        }).filter(
                alias -> !StringUtils.isEmpty(alias) && (StringUtils.isEmpty(request.getFilterModelName()) || alias
                        .toLowerCase(Locale.ROOT).contains(request.getFilterModelName().toLowerCase(Locale.ROOT))))
                .limit(size).collect(Collectors.toList());
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
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();

        QueryStatistics queryStatistics = queryHistoryDAO.getQueryCountAndAvgDuration(startTime, endTime, project);
        return new QueryStatisticsResponse(queryStatistics.getCount(), queryStatistics.getMeanDuration());
    }

    public long getLastWeekQueryCount(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        long endTime = TimeUtil.getDayStart(System.currentTimeMillis());
        long startTime = endTime - 7 * DateUtils.MILLIS_PER_DAY;
        QueryStatistics statistics = queryHistoryDAO.getQueryCountByRange(startTime, endTime, project);
        return statistics.getCount();
    }

    public long getQueryCountToAccelerate(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryIdOffset queryHistoryIdOffset = QueryHistoryIdOffsetManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).get();
        long idOffset = queryHistoryIdOffset.getQueryHistoryIdOffset();
        QueryHistoryDAO queryHistoryDao = getQueryHistoryDao();
        return queryHistoryDao.getQueryHistoryCountBeyondOffset(idOffset, project);
    }

    public Map<String, Object> getQueryCount(String project, long startTime, long endTime, String dimension) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        List<QueryStatistics> queryStatistics;

        if (dimension.equals("model")) {
            queryStatistics = queryHistoryDAO.getQueryCountByModel(startTime, endTime, project);
            return transformQueryStatisticsByModel(project, queryStatistics, "count");
        }

        queryStatistics = queryHistoryDAO.getQueryCountByTime(startTime, endTime, dimension, project);
        fillZeroForQueryStatistics(queryStatistics, startTime, endTime, dimension);
        return transformQueryStatisticsByTime(queryStatistics, "count", dimension);
    }

    public Map<String, Object> getAvgDuration(String project, long startTime, long endTime, String dimension) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        List<QueryStatistics> queryStatistics;

        if (dimension.equals("model")) {
            queryStatistics = queryHistoryDAO.getAvgDurationByModel(startTime, endTime, project);
            return transformQueryStatisticsByModel(project, queryStatistics, "meanDuration");
        }

        queryStatistics = queryHistoryDAO.getAvgDurationByTime(startTime, endTime, dimension, project);
        fillZeroForQueryStatistics(queryStatistics, startTime, endTime, dimension);
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
            Unsafe.changeAccessibleObject(field, true);
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
                TimeZone timeZone = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone());
                LocalDate date = singleStatistics.getTime().atZone(timeZone.toZoneId()).toLocalDate();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM",
                        Locale.getDefault(Locale.Category.FORMAT));
                result.put(date.withDayOfMonth(1).format(formatter), getValueByField(singleStatistics, fieldName));
                return;
            }
            long time = singleStatistics.getTime().toEpochMilli();
            Date date = new Date(time);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
            result.put(sdf.format(date), getValueByField(singleStatistics, fieldName));
        });

        return result;
    }

    public Map<String, String> getQueryHistoryTableMap(List<String> projects) {
        List<String> filterProjects = getProjectManager().listAllProjects().stream().map(ProjectInstance::getName)
                .filter(s -> projects == null || projects.stream().map(str -> str.toLowerCase(Locale.ROOT))
                        .collect(Collectors.toList()).contains(s.toLowerCase(Locale.ROOT)))
                .collect(Collectors.toList());

        Map<String, String> result = Maps.newHashMap();
        for (String project : filterProjects) {
            aclEvaluate.checkProjectReadPermission(project);
            Preconditions.checkArgument(StringUtils.isNotEmpty(project));
            ProjectInstance projectInstance = getProjectManager().getProject(project);
            if (projectInstance == null) {
                throw new KylinException(PROJECT_NOT_EXIST, project);
            }
            result.put(project, getQueryHistoryDao().getQueryMetricMeasurement());
        }

        return result;
    }

}
