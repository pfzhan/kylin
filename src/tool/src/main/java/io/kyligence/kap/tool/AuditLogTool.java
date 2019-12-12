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

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuditLogTool extends ExecutableApplication {

    private static final Option OPTION_START_TIME = OptionBuilder.hasArg().withArgName("START_TIMESTAMP")
            .withDescription("Specify the start timestamp (sec) (optional)").isRequired(false).create("startTime");

    private static final Option OPTION_END_TIME = OptionBuilder.hasArg().withArgName("END_TIMESTAMP")
            .withDescription("Specify the end timestamp (sec) (optional)").isRequired(false).create("endTime");

    private static final Option OPTION_JOB = OptionBuilder.hasArg().withArgName("JOB_ID")
            .withDescription("Specify the job (optional)").isRequired(false).create("job");

    private static final Option OPTION_PROJECT = OptionBuilder.hasArg().withArgName("OPTION_PROJECT")
            .withDescription("Specify project (optional)").isRequired(false).create("project");

    private static final Option OPTION_RESTORE = OptionBuilder.withDescription("Restore audit log from local path")
            .isRequired(false).create("restore");

    private static final Option OPTION_TABLE = OptionBuilder.hasArg().withArgName("TABLE_NAME")
            .withDescription("Specify the table (optional)").isRequired(false).create("table");

    private static final Option OPTION_DIR = OptionBuilder.hasArg().withArgName("DESTINATION_DIR")
            .withDescription("Specify the directory for audit log backup or restore").isRequired(true).create("dir");

    private static final int BATCH_SIZE = 200;

    private static final String AUDIT_LOG_SUFFIX = ".jsonl";

    private final Options options;

    private final KylinConfig kylinConfig;

    AuditLogTool() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public AuditLogTool(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.options = new Options();
        initOptions();
    }

    private void initOptions() {
        options.addOption(OPTION_JOB);
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_START_TIME);
        options.addOption(OPTION_END_TIME);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_RESTORE);
        options.addOption(OPTION_TABLE);
    }

    public static void main(String[] args) {
        val tool = new AuditLogTool();
        tool.execute(args);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String dir = optionsHelper.getOptionValue(OPTION_DIR);
        if (optionsHelper.hasOption(OPTION_RESTORE)) {
            restore(optionsHelper, dir);
        } else if (optionsHelper.hasOption(OPTION_JOB)) {
            extractJob(optionsHelper, dir);
        } else {
            extractFull(optionsHelper, dir);
        }
    }

    private void extractJob(OptionsHelper optionsHelper, final String dir) throws Exception {
        if (!optionsHelper.hasOption(OPTION_PROJECT)) {
            throw new IllegalArgumentException("'-project' specify project name");
        }
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        if (StringUtils.isEmpty(project)) {
            throw new InvalidParameterException("project name shouldn't be empty");
        }

        val jobId = optionsHelper.getOptionValue(OPTION_JOB);
        if (StringUtils.isEmpty(jobId)) {
            throw new InvalidParameterException("job id shouldn't be empty");
        }
        AbstractExecutable job = NExecutableManager.getInstance(kylinConfig, project).getJob(jobId);
        long startTs = job.getStartTime();
        long endTs = job.getEndTime();

        persist(extract(startTs, endTs),
                Paths.get(dir, String.format("%d-%d%s", startTs, endTs, AUDIT_LOG_SUFFIX)).toFile());
    }

    private void extractFull(OptionsHelper optionsHelper, final String dir) throws Exception {
        if (!optionsHelper.hasOption(OPTION_START_TIME)) {
            throw new IllegalArgumentException("'-startTime' specify start timestamp (milliseconds)");
        }
        if (!optionsHelper.hasOption(OPTION_END_TIME)) {
            throw new IllegalArgumentException("'-endTime' specify end timestamp (milliseconds)");
        }
        long startTs = Long.parseLong(optionsHelper.getOptionValue(OPTION_START_TIME));
        long endTs = Long.parseLong(optionsHelper.getOptionValue(OPTION_END_TIME));
        persist(extract(startTs, endTs),
                Paths.get(dir, String.format("%d-%d%s", startTs, endTs, AUDIT_LOG_SUFFIX)).toFile());
    }

    private void restore(OptionsHelper optionsHelper, final String dir) throws Exception {
        if (!optionsHelper.hasOption(OPTION_TABLE)) {
            throw new IllegalArgumentException("'-table' specify table name");
        }
        val table = optionsHelper.getOptionValue(OPTION_TABLE);
        if (StringUtils.isEmpty(table)) {
            throw new InvalidParameterException("table name shouldn't be empty");
        }

        File dirFile = Paths.get(dir).toFile();
        if (!dirFile.exists()) {
            throw new IllegalArgumentException("Directory not exists: " + dir);
        }

        val url = kylinConfig.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        val transactionManager = new DataSourceTransactionManager(dataSource);
        val jdbcTemplate = new JdbcTemplate(dataSource);
        try (JdbcAuditLogStore auditLogStore = new JdbcAuditLogStore(kylinConfig, jdbcTemplate, transactionManager,
                table)) {
            for (File logFile : dirFile.listFiles()) {
                if (!logFile.getName().endsWith(AUDIT_LOG_SUFFIX)) {
                    continue;
                }
                String line;
                try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
                    List<AuditLog> auditLogs = Lists.newArrayList();
                    while ((line = br.readLine()) != null) {
                        try {
                            auditLogs.add(JsonUtil.readValue(line, AuditLog.class));
                        } catch (Exception e) {
                            log.error("audit log deserialize error >>> {}", line, e);
                        }
                    }
                    auditLogStore.batchInsert(auditLogs);
                }
            }
        }
    }

    private void persist(List<AuditLog> auditLogs, File auditLogFile) throws Exception {
        auditLogFile.getParentFile().mkdirs();
        try (final BufferedWriter bw = new BufferedWriter(new FileWriter(auditLogFile))) {
            auditLogs.forEach(x -> {
                try {
                    bw.write(JsonUtil.writeValueAsString(x));
                    bw.newLine();
                } catch (Exception e) {
                    log.error("audit log serialize error, id={}", x.getId(), e);
                }
            });
        }
    }

    private List<AuditLog> extract(long startTs, long endTs) throws Exception {
        List<AuditLog> result = Lists.newArrayList();
        long fromId = 0L;
        try (JdbcAuditLogStore auditLogStore = new JdbcAuditLogStore(kylinConfig)) {
            while (true) {
                List<AuditLog> auditLogs = auditLogStore.fetchRange(fromId, startTs, endTs, BATCH_SIZE);
                if (CollectionUtils.isEmpty(auditLogs)) {
                    break;
                }

                result.addAll(auditLogs);

                if (auditLogs.size() < BATCH_SIZE) {
                    break;
                }
                fromId = auditLogs.get(auditLogs.size() - 1).getId();
            }
        }
        return result;
    }
}