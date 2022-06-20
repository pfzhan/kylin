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
import java.util.Locale;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.ShellException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.ChainedExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import lombok.val;

public class YarnApplicationTool extends ExecutableApplication {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("DESTINATION_DIR")
            .withDescription("Specify the file to save yarn application id").isRequired(true).create("dir");

    private static final Option OPTION_JOB = OptionBuilder.getInstance().hasArg().withArgName("JOB_ID")
            .withDescription("Specify the job").isRequired(true).create("job");

    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("OPTION_PROJECT")
            .withDescription("Specify project").isRequired(true).create("project");

    private final Options options;

    private final KylinConfig kylinConfig;

    YarnApplicationTool() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public YarnApplicationTool(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.options = new Options();
        initOptions();
    }

    private void initOptions() {
        options.addOption(OPTION_JOB);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_DIR);
    }

    public static void main(String[] args) {
        val tool = new YarnApplicationTool();
        tool.execute(args);
        System.out.println("Yarn application task finished.");
        Unsafe.systemExit(0);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String dir = optionsHelper.getOptionValue(OPTION_DIR);
        val jobId = optionsHelper.getOptionValue(OPTION_JOB);
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        AbstractExecutable job = ExecutableManager.getInstance(kylinConfig, project).getJob(jobId);
        if (job instanceof ChainedExecutable) {
            FileUtils.writeLines(new File(dir), extract(project, jobId));
        }
    }

    private Set<String> extract(String project, String jobId) {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getYarnApplicationJobs(jobId);
    }

    public void extractYarnLogs(File exportDir, String project, String jobId) {
        try {
            File yarnLogsDir = new File(exportDir, "yarn_application_log");
            FileUtils.forceMkdir(yarnLogsDir);

            AbstractExecutable job = ExecutableManager.getInstance(kylinConfig, project).getJob(jobId);
            if (!(job instanceof ChainedExecutable)) {
                logger.warn("job type is not ChainedExecutable!");
                return;
            }

            Set<String> applicationIdList = extract(project, jobId);
            if (CollectionUtils.isEmpty(applicationIdList)) {
                String message = "Yarn task submission failed, please check whether yarn is running normally.";
                logger.error(message);
                FileUtils.write(new File(yarnLogsDir, "failed_yarn_application.log"), message);
                return;
            }

            CliCommandExecutor cmdExecutor = new CliCommandExecutor();
            String cmd = "yarn logs -applicationId %s";
            for (String applicationId : applicationIdList) {
                try {
                    if (!applicationId.startsWith("application")) {
                        continue;
                    }
                    val result = cmdExecutor.execute(String.format(Locale.ROOT, cmd, applicationId), null);

                    if (result.getCode() != 0) {
                        logger.error("Failed to execute the yarn cmd: {}", cmd);
                    }

                    if (null != result.getCmd()) {
                        FileUtils.write(new File(yarnLogsDir, applicationId + ".log"), result.getCmd());
                    }
                } catch (ShellException se) {
                    logger.error("Failed to extract log by yarn job: {}", applicationId, se);
                    String detailMessage = se.getMessage()
                            + "\n For detailed error information, please see logs/diag.log or logs/kylin.log";
                    FileUtils.write(new File(yarnLogsDir, applicationId + ".log"), detailMessage);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to extract yarn job logs.", e);
        }
    }

}