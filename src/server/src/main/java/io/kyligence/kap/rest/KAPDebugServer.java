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

package io.kyligence.kap.rest;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kylingence.kap.event.handle.AddSegmentHandler;
import io.kylingence.kap.event.handle.LoadingRangeUpdateHandler;
import io.kylingence.kap.event.handle.ProjectHandler;
import io.kylingence.kap.event.handle.RemoveSegmentHandler;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

import java.io.File;
import java.util.UUID;

@ImportResource(locations = { "applicationContext.xml", "kylinSecurity.xml" })
@SpringBootApplication
public class KAPDebugServer {

    private static File localMetadata;

    public static void main(String[] args) throws SchedulerException {
        setLocalEnvs();
        SpringApplication.run(KAPDebugServer.class, args);
        new LoadingRangeUpdateHandler();
        new AddSegmentHandler();
        new ProjectHandler();
        new RemoveSegmentHandler();
        new NDefaultScheduler("default").init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (localMetadata != null && localMetadata.exists()) {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    localMetadata.delete();
                }
            }));
        }
    }

    private static void setSandboxEnvs() {
        KylinConfig.setSandboxEnvIfPossible();
    }

    private static void setLocalEnvs() {
        if ((localMetadata = new File(TempMetadataBuilder.TEMP_TEST_METADATA)).exists()) {
            localMetadata.delete();
        }
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);

        localMetadata = new File(tempMetadataDir);
        final SparkConf sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        SparkSession.builder().config(sparkConf).getOrCreate();
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
    }

}
