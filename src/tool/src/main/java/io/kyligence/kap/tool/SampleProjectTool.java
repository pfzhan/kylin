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

import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_PROJECT_NAME;

import java.util.Locale;

import io.kyligence.kap.common.util.AddressUtil;
import lombok.val;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;

import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.util.ToolMainWrapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SampleProjectTool extends ExecutableApplication {

    private void checkProject(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance prjInstance = projectManager.getProject(project);
        if (prjInstance != null) {
            throw new KylinException(DUPLICATE_PROJECT_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getPROJECT_ALREADY_EXIST(), project));
        }
    }

    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("PROJECT_NAME")
            .isRequired(true).create("project");

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("DIRECTORY_PATH")
            .isRequired(true).create("dir");

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_DIR);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String project = optionsHelper.getOptionValue(OPTION_PROJECT);
        checkProject(project);
        String dir = optionsHelper.getOptionValue(OPTION_DIR);
        val config = KylinConfig.getInstanceFromEnv();
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        resourceStore.getAuditLogStore().setInstance(AddressUtil.getMockPortAddress());
        MetadataTool tool = new MetadataTool(config);
        tool.execute(new String[] { "-restore", "-dir", dir, "-project", project, "--after-truncate" });
    }

    public static void main(String[] args) {
        ToolMainWrapper.wrap(args, () -> {
            SampleProjectTool tool = new SampleProjectTool();
            tool.execute(args);
        });
        Unsafe.systemExit(0);
    }
}
