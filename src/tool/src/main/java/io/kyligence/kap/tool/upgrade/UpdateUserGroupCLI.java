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
package io.kyligence.kap.tool.upgrade;

import static io.kyligence.kap.tool.util.MetadataUtil.getMetadataUrl;
import static io.kyligence.kap.tool.util.ScreenPrintUtil.printlnGreen;
import static io.kyligence.kap.tool.util.ScreenPrintUtil.systemExitWhenMainThread;
import static org.apache.kylin.common.exception.ServerErrorCode.USERGROUP_NOT_EXIST;
import static org.apache.kylin.common.persistence.ResourceStore.USER_GROUP_ROOT;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.metadata.usergroup.UserGroup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * 4.1 -> 4.2
 */
@Slf4j
public class UpdateUserGroupCLI extends ExecutableApplication {

    private static final Option OPTION_METADATA_DIR = OptionBuilder.getInstance().hasArg().withArgName("metadata_dir")
            .withDescription("metadata dir.").isRequired(true).withLongOpt("metadata_dir").create("d");

    private static final Option OPTION_EXEC = OptionBuilder.getInstance().hasArg(false).withArgName("exec")
            .withDescription("exec the upgrade.").isRequired(false).withLongOpt("exec").create("e");

    @Data
    private static class UserGroup4Dot1 {
        @JsonProperty("groups")
        List<String> groups;
    }

    public static void main(String[] args) {
        log.info("Start upgrade user group metadata.");
        try {
            UpdateUserGroupCLI tool = new UpdateUserGroupCLI();
            tool.execute(args);
        } catch (Exception e) {
            log.error("Upgrade user group metadata failed.", e);
            systemExitWhenMainThread(1);
        }
        log.info("Upgrade user group metadata successfully.");
        systemExitWhenMainThread(0);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_METADATA_DIR);
        options.addOption(OPTION_EXEC);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String metadataUrl = getMetadataUrl(optionsHelper.getOptionValue(OPTION_METADATA_DIR));
        Preconditions.checkArgument(StringUtils.isNotBlank(metadataUrl));
        File userGroupFile = new File(metadataUrl, USER_GROUP_ROOT);
        if (!userGroupFile.exists()) {
            throw new KylinException(USERGROUP_NOT_EXIST, "User group metadata doesn't exist.");
        }
        if (userGroupFile.isDirectory()) {
            printlnGreen("user group metadata upgrade succeeded.");
            log.info("Succeed to upgrade user group metadata.");
            return;
        }
        printlnGreen("found user group metadata need to be upgraded.");
        if (optionsHelper.hasOption(OPTION_EXEC)) {
            UserGroup4Dot1 userGroups = JsonUtil.readValue(userGroupFile, UserGroup4Dot1.class);
            FileUtils.forceDelete(userGroupFile);
            userGroupFile.mkdirs();
            for (String groupName : userGroups.getGroups()) {
                File out = new File(userGroupFile, groupName);
                UserGroup userGroup = new UserGroup(groupName);
                JsonUtil.writeValue(out, userGroup);
            }
            printlnGreen("user group metadata upgrade succeeded.");
            log.info("Succeed to upgrade user group metadata.");
        }
    }

}
