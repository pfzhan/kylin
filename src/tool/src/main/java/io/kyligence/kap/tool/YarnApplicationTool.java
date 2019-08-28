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

import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_ID;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_URL;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;

import com.google.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class YarnApplicationTool extends ExecutableApplication {
    private static final Option OPTION_DIR = OptionBuilder.hasArg().withArgName("DESTINATION_DIR")
            .withDescription("Specify the file to save yarn application id").isRequired(true).create("dir");

    private static final Option OPTION_JOB = OptionBuilder.hasArg().withArgName("JOB_ID")
            .withDescription("Specify the job").isRequired(true).create("job");

    private static final Option OPTION_PROJECT = OptionBuilder.hasArg().withArgName("OPTION_PROJECT")
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
        AbstractExecutable job = NExecutableManager.getInstance(kylinConfig, project).getJob(jobId);
        if (job instanceof ChainedExecutable) {
            FileUtils.writeLines(new File(dir), extract((ChainedExecutable) job));
        }
    }

    private List<String> extract(ChainedExecutable job) {
        final List<String> applications = Lists.newArrayList();
        for (AbstractExecutable task : job.getTasks()) {
            Output output = task.getOutput();
            if (null == output || null == output.getExtra()) {
                continue;
            }
            String applicationId = output.getExtra().get(YARN_APP_ID);
            if (StringUtils.isEmpty(applicationId)) {
                String trackingUrl = task.getOutput().getExtra().get(YARN_APP_URL);
                if (StringUtils.isEmpty(trackingUrl)) {
                    continue;
                }
                trackingUrl = StringUtils.stripEnd(trackingUrl, "/");
                applicationId = trackingUrl.substring(trackingUrl.lastIndexOf("/") + 1);
            }
            applications.add(applicationId);
        }
        return applications;
    }

    public void extractYarnLogs(File exportDir, String project, String jobId) {
        try {
            File yarnLogsDir = new File(exportDir, "yarn_application_log");
            FileUtils.forceMkdir(yarnLogsDir);

            AbstractExecutable job = NExecutableManager.getInstance(kylinConfig, project).getJob(jobId);
            if (!(job instanceof ChainedExecutable)) {
                log.warn("job type is not ChainedExecutable!");
                return;
            }

            List<String> applicationIdList = extract((ChainedExecutable) job);

            CliCommandExecutor cmdExecutor = new CliCommandExecutor();
            String cmd = "yarn logs -applicationId %s";
            for (String applicationId : applicationIdList) {
                try {
                    Pair<Integer, String> result = cmdExecutor.execute(String.format(cmd, applicationId),
                            null);

                    if (result.getFirst() != 0) {
                        log.error("Failed to execute the yarn cmd: {}", cmd);
                    }

                    if (null != result.getSecond()) {
                        FileUtils.write(new File(yarnLogsDir, applicationId + ".log"), result.getSecond());
                    }
                } catch (ShellException se) {
                    log.error("Failed to extract log by yarn job: {}", applicationId, se);
                    FileUtils.write(new File(yarnLogsDir, applicationId + ".log"), se.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Failed to extract yarn job logs.", e);
        }
    }

}