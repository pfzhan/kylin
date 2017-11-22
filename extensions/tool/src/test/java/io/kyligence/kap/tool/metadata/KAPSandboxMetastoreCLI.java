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

package io.kyligence.kap.tool.metadata;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;

/**
 * This is a helper class for developer to directly manipulate the metadata store in sandbox
 * This is designed to run in IDE(i.e. not on sandbox hadoop CLI)
 *
 * For production metadata store manipulation refer to bin/metastore.sh in binary package
 * It is desinged to run in hadoop CLI, both in sandbox or in real hadoop environment
 */
public class KAPSandboxMetastoreCLI {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");
        System.out.println("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, HBaseMetadataTestCase.SANDBOX_TEST_DATA);
        if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
            throw new RuntimeException(
                    "No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.4.0.0-169");
        }

        if (args.length < 1) {
            printUsage();
            return;
        }

        if ("reset".equalsIgnoreCase(args[0])) {
            ResourceTool.main(new String[] { "reset" });
        } else if ("download".equalsIgnoreCase(args[0])) {
            ResourceTool.main(new String[] { "download", args[1] });
        } else if ("fetch".equalsIgnoreCase(args[0])) {
            ResourceTool.main(new String[] { "fetch", args[1], args[2] });
        } else if ("upload".equalsIgnoreCase(args[0])) {
            ResourceTool.main(new String[] { "upload", args[1] });
        } else {
            printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Usage: SandboxMetastoreCLI download toFolder");
        System.out.println("Usage: SandboxMetastoreCLI fecth toFolder data");
        System.out.println("Usage: SandboxMetastoreCLI upload   fromFolder");
    }
}
