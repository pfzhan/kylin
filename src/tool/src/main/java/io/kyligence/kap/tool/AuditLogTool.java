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
import static io.kyligence.kap.tool.garbage.StorageCleaner.ANSI_RED;
import static io.kyligence.kap.tool.garbage.StorageCleaner.ANSI_RESET;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_NOT_SPECIFY;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_TIMESTAMP_NOT_SPECIFY;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.PATH_NOT_EXISTS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.constant.Constant;
import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import lombok.val;

public class AuditLogTool extends ExecutableApplication {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final String CHARSET = Charset.defaultCharset().name();

    private static final Option OPTION_START_TIME = OptionBuilder.getInstance().hasArg().withArgName("START_TIMESTAMP")
            .withDescription("Specify the start timestamp (sec) (optional)").isRequired(false).create("startTime");

    private static final Option OPTION_END_TIME = OptionBuilder.getInstance().hasArg().withArgName("END_TIMESTAMP")
            .withDescription("Specify the end timestamp (sec) (optional)").isRequired(false).create("endTime");

    private static final Option OPTION_JOB = OptionBuilder.getInstance().hasArg().withArgName("JOB_ID")
            .withDescription("Specify the job (optional)").isRequired(false).create("job");

    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("OPTION_PROJECT")
            .withDescription("Specify project (optional)").isRequired(false).create("project");

    private static final Option OPTION_RESTORE = OptionBuilder.getInstance()
            .withDescription("Restore audit log from local path").isRequired(false).create("restore");

    private static final Option OPTION_TABLE = OptionBuilder.getInstance().hasArg().withArgName("TABLE_NAME")
            .withDescription("Specify the table (optional)").isRequired(false).create("table");

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("DESTINATION_DIR")
            .withDescription("Specify the directory for audit log backup or restore").isRequired(true).create("dir");

    private static final String AUDIT_LOG_SUFFIX = ".jsonl";

    private static final int MAX_BATCH_SIZE = 50000;

    private final Options options;

    private final KylinConfig kylinConfig;

    AuditLogTool() {
        this(KylinConfig.newKylinConfig());
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
        try {
            val tool = new AuditLogTool();
            tool.execute(args);
        } catch (Exception e) {
            System.out.println(ANSI_RED + "Audit log task failed." + ANSI_RESET);
            logger.error("fail execute audit log tool: ", e);
            Unsafe.systemExit(1);
        }
        System.out.println("Audit log task finished.");
        Unsafe.systemExit(0);
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
            throw new KylinException(PARAMETER_NOT_SPECIFY, "-project");
        }
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        if (StringUtils.isEmpty(project)) {
            throw new KylinException(PARAMETER_EMPTY, "project");
        }

        val jobId = optionsHelper.getOptionValue(OPTION_JOB);
        if (StringUtils.isEmpty(jobId)) {
            throw new KylinException(PARAMETER_EMPTY, "job");
        }
        AbstractExecutable job = ExecutableManager.getInstance(kylinConfig, project).getJob(jobId);
        long startTs = job.getStartTime();
        long endTs = job.getEndTime();

        extract(startTs, endTs,
                Paths.get(dir, String.format(Locale.ROOT, "%d-%d%s", startTs, endTs, AUDIT_LOG_SUFFIX)).toFile());
    }

    private void extractFull(OptionsHelper optionsHelper, final String dir) throws Exception {
        if (!optionsHelper.hasOption(OPTION_START_TIME)) {
            throw new KylinException(PARAMETER_TIMESTAMP_NOT_SPECIFY, "-startTime");
        }
        if (!optionsHelper.hasOption(OPTION_END_TIME)) {
            throw new KylinException(PARAMETER_TIMESTAMP_NOT_SPECIFY, "-endTime");
        }
        long startTs = Long.parseLong(optionsHelper.getOptionValue(OPTION_START_TIME));
        long endTs = Long.parseLong(optionsHelper.getOptionValue(OPTION_END_TIME));

        extract(startTs, endTs,
                Paths.get(dir, String.format(Locale.ROOT, "%d-%d%s", startTs, endTs, AUDIT_LOG_SUFFIX)).toFile());
    }

    private void restore(OptionsHelper optionsHelper, final String dir) throws Exception {
        if (!optionsHelper.hasOption(OPTION_TABLE)) {
            throw new KylinException(PARAMETER_NOT_SPECIFY, "-table");
        }
        val table = optionsHelper.getOptionValue(OPTION_TABLE);
        if (StringUtils.isEmpty(table)) {
            throw new KylinException(PARAMETER_EMPTY, "table");
        }

        File dirFile = Paths.get(dir).toFile();
        if (!dirFile.exists()) {
            throw new KylinException(PATH_NOT_EXISTS, dir);
        }

        val url = kylinConfig.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        val transactionManager = new DataSourceTransactionManager(dataSource);
        val jdbcTemplate = new JdbcTemplate(dataSource);
        try (JdbcAuditLogStore auditLogStore = new JdbcAuditLogStore(kylinConfig, jdbcTemplate, transactionManager,
                table)) {
            for (File logFile : Objects.requireNonNull(dirFile.listFiles())) {
                if (!logFile.getName().endsWith(AUDIT_LOG_SUFFIX)) {
                    continue;
                }
                String line;
                try (InputStream fin = new FileInputStream(logFile);
                        BufferedReader br = new BufferedReader(new InputStreamReader(fin, CHARSET))) {
                    List<AuditLog> auditLogs = Lists.newArrayList();
                    while ((line = br.readLine()) != null) {
                        try {
                            auditLogs.add(JsonUtil.readValue(line, AuditLog.class));
                        } catch (Exception e) {
                            logger.error("audit log deserialize error >>> {}", line, e);
                        }
                    }
                    auditLogStore.batchInsert(auditLogs);
                }
            }
        }
    }

    private void extract(long startTs, long endTs, File auditLogFile) throws Exception {
        auditLogFile.getParentFile().mkdirs();
        long fromId = Long.MAX_VALUE;
        int batchSize = KylinConfig.getInstanceFromEnv().getAuditLogBatchSize();
        //Prevent OOM
        batchSize = Math.min(MAX_BATCH_SIZE, batchSize);
        logger.info("Audit log batch size is {}.", batchSize);
        try (JdbcAuditLogStore auditLogStore = new JdbcAuditLogStore(kylinConfig,
                kylinConfig.getAuditLogBatchTimeout());
                OutputStream fos = new FileOutputStream(auditLogFile);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, CHARSET),
                        Constant.AUDIT_MAX_BUFFER_SIZE)) {
            while (true) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("audit log task is interrupt");
                }
                List<AuditLog> auditLogs = auditLogStore.fetchRange(fromId, startTs, endTs, batchSize);
                if (CollectionUtils.isEmpty(auditLogs)) {
                    break;
                }
                auditLogs.forEach(x -> {
                    try {
                        bw.write(JsonUtil.writeValueAsString(x));
                        bw.newLine();
                    } catch (Exception e) {
                        logger.error("Write audit log error, id is {}", x.getId(), e);
                    }
                });

                if (auditLogs.size() < batchSize) {
                    break;
                }
                fromId = auditLogs.get(auditLogs.size() - 1).getId();
                logger.info("Audit log size is {}, id range is [{},{}].", auditLogs.size(), auditLogs.get(0).getId(),
                        fromId);
            }
        }
    }
}