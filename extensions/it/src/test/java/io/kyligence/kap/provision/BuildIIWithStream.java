/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package io.kyligence.kap.provision;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.KAPDeployUtil;

public class BuildIIWithStream extends org.apache.kylin.provision.BuildIIWithStream {

    private static final Logger logger = LoggerFactory.getLogger(org.apache.kylin.provision.BuildIIWithStream.class);

    public static void main(String[] args) throws Exception {
        try {
            beforeClass();
            BuildIIWithStream buildCubeWithEngine = new BuildIIWithStream();
            buildCubeWithEngine.before();
            buildCubeWithEngine.build();
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
        KAPDeployUtil.overrideJobJarLocations();
    }
}
