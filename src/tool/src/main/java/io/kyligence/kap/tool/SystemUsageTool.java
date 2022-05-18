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

package io.kyligence.kap.tool;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.guava20.shaded.common.base.MoreObjects;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryDailyStatistic;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord;

public class SystemUsageTool {
    private SystemUsageTool() {
    }

    private static final Logger logger = LoggerFactory.getLogger("diag");
    private static final String QUERY_DAILY_FILE_NAME = "query_daily.csv";
    private static final String BUILD_DAILY_FILE_NAME = "build_daily.csv";
    private static final String BASE_FILE_NAME = "base";

    public static void extractUseInfo(File exportDir, long startTime, long endTime) {
        File destDir = new File(exportDir, "system_usage");

        try {
            FileUtils.forceMkdir(destDir);
            baseInfo(destDir);
            queryDailyInfo(destDir, startTime, endTime);
            buildDailyInfo(destDir, startTime, endTime);
        } catch (Exception e) {
            logger.error("Failed to extract system usage", e);
        }
    }

    private static void queryDailyInfo(File exportDir, long startTime, long endTime) throws IOException {
        RDBMSQueryHistoryDAO queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        List<QueryDailyStatistic> queryDailyStatisticList = queryHistoryDAO.getQueryDailyStatistic(startTime, endTime);
        List<String> lines = Lists.newArrayList();
        lines.add(
                "date,active_users,number_of_queries,number_of_successful_queries,average_time_spent_seconds,number_of_queries_within_1s,number_of_queries_within_3s,number_of_queries_within_5s,number_of_queries_within_10s,number_of_queries_within_15s");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        queryDailyStatisticList.forEach(e -> lines.add(String.format(Locale.ROOT, "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                simpleDateFormat.format(new Date(e.getQueryDay())), e.getActiveUserNum(), e.getTotalNum(),
                e.getSucceedNum(), String.format("%.1f", e.getAvgDuration() / 1000.0), e.getLt1sNum(), e.getLt3sNum(),
                e.getLt5sNum(), e.getLt10sNum(), e.getLt15sNum())));
        FileUtils.writeLines(new File(exportDir, QUERY_DAILY_FILE_NAME), lines, false);
    }

    private static void buildDailyInfo(File exportDir, long startTime, long endTime) throws IOException {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<ExecutablePO> allJobs = Lists.newArrayList();
        projectManager.listAllProjects().forEach(projectInstance -> {
            NExecutableManager executableManager = KylinConfig.getInstanceFromEnv()
                    .getManager(projectInstance.getName(), NExecutableManager.class);
            allJobs.addAll(executableManager.getAllJobs(startTime, endTime));
        });
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Map<String, List<ExecutablePO>> dailyBuildExecutableMap = allJobs.stream()
                .filter(e -> JobTypeEnum.Category.BUILD.equals(e.getJobType().getCategory()))
                .collect(Collectors.groupingBy(e -> simpleDateFormat.format(new Date(e.getLastModified()))));
        List<String> lines = Lists.newArrayList();
        lines.add("date,number_of_build_tasks,average_build_time_minutes,build_success_rate");
        dailyBuildExecutableMap.entrySet().stream()
                .sorted(Map.Entry.<String, List<ExecutablePO>> comparingByKey().reversed()).forEachOrdered(entry -> {
                    String key = entry.getKey();
                    List<ExecutablePO> value = entry.getValue();
                    int buildNum = CollectionUtils.size(value);
                    long buildSucceedNum = value.stream()
                            .filter(e -> ExecutableState.SUCCEED.name().equals(e.getOutput().getStatus())).count();
                    double totalDuration = value.stream()
                            .filter(e -> ExecutableState.SUCCEED.name().equals(e.getOutput().getStatus()))
                            .mapToDouble(e -> {
                                AbstractExecutable executable = KylinConfig.getInstanceFromEnv()
                                        .getManager(e.getProject(), NExecutableManager.class).fromPO(e);
                                return executable.getWaitTime() + executable.getDuration();
                            }).sum();
                    lines.add(String.format(Locale.ROOT, "%s,%s,%s,%s", key, buildNum,
                            divide(totalDuration, buildSucceedNum * 60.0 * 1000, "%.2f"),
                            divide(buildSucceedNum * 1.0, buildNum, "%.3f")));
                });
        FileUtils.writeLines(new File(exportDir, BUILD_DAILY_FILE_NAME), lines, false);
    }

    public static String divide(double molecular, double denominator, String format) {
        return String.format(format, denominator == 0 ? 0.0 : molecular / denominator);
    }

    private static void baseInfo(File exportDir) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        List<ProjectInstance> allProjects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .listAllProjects();
        stringBuilder.append("project_num : ").append(CollectionUtils.size(allProjects)).append("\n");
        long modelNum = allProjects.stream()
                .map(e -> NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), e.getName()).listAllModels())
                .mapToLong(Collection::size).sum();
        stringBuilder.append("model_num : ").append(modelNum).append("\n");
        SourceUsageRecord latestRecord = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getLatestRecord();
        stringBuilder.append("license_capacity : ")
                .append(FileUtils.byteCountToDisplaySize(
                        MoreObjects.firstNonNull(latestRecord, new SourceUsageRecord()).getLicenseCapacity()))
                .append("\n");
        stringBuilder.append("license_used_capacity : ")
                .append(FileUtils.byteCountToDisplaySize(
                        MoreObjects.firstNonNull(latestRecord, new SourceUsageRecord()).getCurrentCapacity()))
                .append("\n");
        FileUtils.writeStringToFile(new File(exportDir, BASE_FILE_NAME), stringBuilder.toString(),
                StandardCharsets.UTF_8, false);
    }

}
