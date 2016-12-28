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

public class BuildCubeWithEngine extends org.apache.kylin.provision.BuildCubeWithEngine {

    private static final Logger logger = LoggerFactory.getLogger(BuildCubeWithEngine.class);

    public static void main(String[] args) throws Exception {
        try {
            beforeClass();

            BuildCubeWithEngine buildCubeWithEngine = new BuildCubeWithEngine();
            buildCubeWithEngine.before();
            buildCubeWithEngine.build();
            buildCubeWithEngine.after();
            logger.info("Build is done");
            afterClass();
            logger.info("Going to exit");
            System.exit(0);
        } catch (Exception e) {
            logger.error("error", e);
            System.exit(1);
        }
    }

    @Override
    protected void deployEnv() throws IOException {
        KAPDeployUtil.initCliWorkDir();
        KAPDeployUtil.deployMetadata();
        KAPDeployUtil.overrideJobJarLocations();
    }
}