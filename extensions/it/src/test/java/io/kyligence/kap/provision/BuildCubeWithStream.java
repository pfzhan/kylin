/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package io.kyligence.kap.provision;

import io.kyligence.kap.DeployUtil;

import java.io.File;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.streaming.OneOffStreamingBuilder;
import org.apache.kylin.engine.streaming.StreamingConfig;
import org.apache.kylin.engine.streaming.StreamingManager;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  for streaming cubing case "test_streaming_table"
 */
public class BuildCubeWithStream {

    private static final Logger logger = LoggerFactory.getLogger(org.apache.kylin.provision.BuildCubeWithStream.class);
    private static final String cubeName = "test_streaming_table_cube";
    private static final long startTime = DateFormat.stringToMillis("2015-01-01 00:00:00");
    private static final long endTime = DateFormat.stringToMillis("2015-01-03 00:00:00");
    private static final long batchInterval = 16 * 60 * 60 * 1000;//16 hours

    private KylinConfig kylinConfig;

    public static void main(String[] args) throws Exception {

        try {
            beforeClass();

            BuildCubeWithStream buildCubeWithStream = new BuildCubeWithStream();
            buildCubeWithStream.before();
            buildCubeWithStream.build();
            logger.info("Build is done");
            afterClass();
            logger.info("Going to exit");
            System.exit(0);
        } catch (Exception e) {
            logger.error("error", e);
            System.exit(1);
        }

    }

    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, HBaseMetadataTestCase.SANDBOX_TEST_DATA);
        if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
            throw new RuntimeException("No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.2.4.2-2");
        }
        HBaseMetadataTestCase.staticCreateTestMetadata(HBaseMetadataTestCase.SANDBOX_TEST_DATA);
    }

    public void before() throws Exception {
        DeployUtil.overrideJobJarLocations();

        kylinConfig = KylinConfig.getInstanceFromEnv();
        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        final String factTable = cubeInstance.getFactTable();
        final StreamingConfig config = StreamingManager.getInstance(kylinConfig).getStreamingConfig(factTable);

        //Use a random topic for kafka data stream
        KafkaConfig streamingConfig = KafkaConfigManager.getInstance(kylinConfig).getKafkaConfig(config.getName());
        streamingConfig.setTopic(UUID.randomUUID().toString());
        KafkaConfigManager.getInstance(kylinConfig).saveKafkaConfig(streamingConfig);

        org.apache.kylin.job.DeployUtil.prepareTestDataForStreamingCube(startTime, endTime, cubeName, streamingConfig);
    }

    public static void afterClass() throws Exception {
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    public void build() throws Exception {
        logger.info("start time:" + startTime + " end time:" + endTime + " batch interval:" + batchInterval + " batch count:" + ((endTime - startTime) / batchInterval));
        for (long start = startTime; start < endTime; start += batchInterval) {
            logger.info(String.format("build batch:{%d, %d}", start, start + batchInterval));
            new OneOffStreamingBuilder(RealizationType.CUBE, cubeName, start, start + batchInterval).build().run();
        }
    }
}
