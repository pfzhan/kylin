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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.joda.time.format.DateTimeFormat;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.metrics.MetricsInfo;
import io.kyligence.kap.tool.util.ToolMainWrapper;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsInfoTool extends ExecutableApplication {

    private static final String METRIC_DIR = "_metrics/";
    private static final String METRIC_SUFFIX = "_metric.json";
    private static final long DAY_MILLISECOND = 24 * 3600000L;
    private static final String BACKUP_FORMAT = "yyyy-MM-dd-HH-mm-ss";
    private static final String BACKUP_MATCH = "\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}_backup";
    private FileSystem fs;
    private KylinConfig config;
    private String metadataPath;
    private String outPath;
    private String metricDate;
    private String lastMetricDate;

    private static final Option OPTION_DATE = OptionBuilder.getInstance().hasArg().withArgName("METRIC_DATE")
            .withDescription("Specifies the date(yyyyMMdd) on which metrics are calculated").isRequired(true)
            .create("date");

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DATE);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        init(optionsHelper);
        if (locateMetaPath()) {
            MetricsInfo metricsInfo = new MetricsInfo(DateFormat.formatToTimeStr(System.currentTimeMillis()),
                    getProjectMetrics(), getStorageMetric());
            outPutMetricsInfo(metricsInfo);
        } else {
            throw new IllegalAccessException("No backup file for specified date," + metricDate);
        }
    }

    private void init(OptionsHelper optionsHelper) throws IllegalAccessException {
        metricDate = optionsHelper.getOptionValue(OPTION_DATE);
        if (!isValidDate(metricDate)) {
            throw new IllegalAccessException("'" + metricDate + "' is not a valid date of pattern 'yyyyMMdd'");
        }
        long metricDateMillis = DateFormat.stringToDate(metricDate, DateFormat.COMPACT_DATE_PATTERN).getTime();
        lastMetricDate = DateFormat.formatToDateStr(metricDateMillis - DAY_MILLISECOND,
                DateFormat.COMPACT_DATE_PATTERN);
        fs = HadoopUtil.getWorkingFileSystem();
        config = KylinConfig.getInstanceFromEnv();
        metadataPath = config.getHdfsWorkingDirectory();
        outPath = config.getHdfsWorkingDirectory() + METRIC_DIR + "";

    }

    private List<MetricsInfo.ProjectMetric> getProjectMetrics() throws IOException {
        List<MetricsInfo.ProjectMetric> projectMetrics = new ArrayList<>();
        Map<String, Integer> modelMap = getModelMap(inPutMetricsInfo(lastMetricDate));
        NProjectManager.getInstance(config).listAllProjects().forEach(projectInstance -> {
            String name = projectInstance.getName();
            int modelTotalCount = NDataModelManager.getInstance(config, name).listAllModels().size();
            Integer lastModelCount = modelMap.get(name);
            Integer modelAddCount = lastModelCount == null ? null : (modelTotalCount - lastModelCount);
            MetricsInfo.ProjectMetric projectMetric = new MetricsInfo.ProjectMetric(name, modelTotalCount,
                    modelAddCount);
            projectMetrics.add(projectMetric);
        });
        return projectMetrics;
    }

    private boolean locateMetaPath() throws IOException {
        val path = new Path(HadoopUtil.getBackupFolder(config));
        if (!fs.exists(path)) {
            log.error("check default backup folder failed");
            return false;
        }
        val formatter = DateTimeFormat.forPattern(BACKUP_FORMAT);
        val metricDateMillis = DateFormat.stringToDate(metricDate, DateFormat.COMPACT_DATE_PATTERN).getTime();
        val candidateFolder = Arrays.stream(fs.listStatus(path))
                .filter(file -> Pattern.matches(BACKUP_MATCH, file.getPath().getName())).filter(file -> {
                    val filePrefix = file.getPath().getName().substring(0, BACKUP_FORMAT.length());
                    long fileMillis = formatter.parseDateTime(filePrefix).getMillis();
                    return fileMillis >= metricDateMillis && fileMillis < metricDateMillis + DAY_MILLISECOND;
                }).collect(Collectors.toSet());

        if (candidateFolder.isEmpty()) {
            log.error("check default backup folder failed");
            return false;
        }
        val last = candidateFolder.stream().max(Comparator.comparingLong(FileStatus::getModificationTime));
        val folder = last.get().getPath().getName();
        val restorePath = StringUtils.appendIfMissing(HadoopUtil.getBackupFolder(config), "/") + folder;
        config.setProperty("kylin.metadata.url", config.getMetadataUrlPrefix() + "@hdfs,path=" + restorePath);
        return true;
    }

    private Map<String, Integer> getModelMap(MetricsInfo info) {
        Map<String, Integer> map = Maps.newHashMap();
        if (info != null && info.getProjectMetrics() != null) {
            info.getProjectMetrics().forEach(metric -> map.put(metric.getProjectName(), metric.getModelTotalCount()));
        }
        return map;
    }

    @VisibleForTesting
    public MetricsInfo inPutMetricsInfo(String dateStr) throws IOException {
        Path path = new Path(outPath + dateStr + METRIC_SUFFIX);
        if (fs.exists(path)) {
            try (FSDataInputStream inputStream = fs.open(path)) {
                return JsonUtil.readValue(inputStream, MetricsInfo.class);
            }
        }
        return null;
    }

    private void outPutMetricsInfo(MetricsInfo metricsInfo) throws IOException {
        String pathStr = outPath + metricDate + METRIC_SUFFIX;
        Path path = new Path(pathStr);
        try (FSDataOutputStream outputStream = fs.create(path)) {
            outputStream.writeBytes(JsonUtil.writeValueAsString(metricsInfo));
            log.info("outPut MetricsInfo success at {}", pathStr);
        }
    }

    private MetricsInfo.StorageMetric getStorageMetric() throws IOException {
        ContentSummary contentSummary = fs.getContentSummary(new Path(metadataPath));
        return new MetricsInfo.StorageMetric(metadataPath, contentSummary.getLength(), contentSummary.getFileCount(),
                contentSummary.getDirectoryCount());
    }

    public static void main(String[] args) {
        ToolMainWrapper.wrap(args, () -> {
            MetricsInfoTool tool = new MetricsInfoTool();
            tool.execute(args);
        });
        Unsafe.systemExit(0);
    }

    private boolean isValidDate(String metricDate) {
        try {
            if (metricDate.length() == 8 && String.valueOf(Integer.parseInt(metricDate)).length() == 8) {
                SimpleDateFormat format = new SimpleDateFormat(DateFormat.COMPACT_DATE_PATTERN,
                        Locale.getDefault(Locale.Category.FORMAT));
                format.setLenient(false);
                format.parse(metricDate);
                return true;
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return false;
    }

}
