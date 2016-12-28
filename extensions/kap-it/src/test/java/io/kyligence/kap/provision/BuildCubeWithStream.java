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

package io.kyligence.kap.provision;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.KAPDeployUtil;
import io.kyligence.kap.tool.storage.KapStorageCleanupCLI;

/**
 *  for streaming cubing case "test_streaming_table"
 */
public class BuildCubeWithStream extends org.apache.kylin.provision.BuildCubeWithStream {

    private static final Logger logger = LoggerFactory.getLogger(org.apache.kylin.provision.BuildCubeWithStream.class);

    public static void main(String[] args) throws Exception {
        BuildCubeWithStream buildCubeWithStream = null;
        try {
            beforeClass();
            buildCubeWithStream = new BuildCubeWithStream();
            buildCubeWithStream.before();
            buildCubeWithStream.build();
            logger.info("Build is done");
            buildCubeWithStream.cleanup();
            logger.info("Going to exit");
            System.exit(0);
        } catch (Throwable e) {
            logger.error("error", e);
            System.exit(1);
        }

    }

    protected void deployEnv() throws IOException {
        KAPDeployUtil.overrideJobJarLocations();
    }

    @Override
    protected void cleanupOldStorage() throws Exception {
        String[] args = { "--delete", "true" };

        KapStorageCleanupCLI cli = new KapStorageCleanupCLI();
        cli.execute(args);
    }

}
