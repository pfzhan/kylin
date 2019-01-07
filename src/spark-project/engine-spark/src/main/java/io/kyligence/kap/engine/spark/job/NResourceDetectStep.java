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

package io.kyligence.kap.engine.spark.job;

import java.nio.file.Paths;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;

public class NResourceDetectStep extends NSparkExecutable {
    private static final Logger logger = LoggerFactory.getLogger(NSparkExecutable.class);

    public NResourceDetectStep() {
    }

    public NResourceDetectStep(DefaultChainedExecutable parent) {
        if (parent instanceof NSparkCubingJob) {
            this.setSparkSubmitClassName(ResourceDetectBeforeCubingJob.class.getName());
        } else {
            this.setSparkSubmitClassName(ResourceDetectBeforeMergingJob.class.getName());
        }
        this.setName(ExecutableConstants.STEP_NAME_DETECT_RESOURCE);
    }

    protected Set<String> getMetadataDumpList(KylinConfig config) {
        NDataflow df = NDataflowManager.getInstance(config, getProject()).getDataflow(getDataflowId());
        return df.collectPrecalculationResource();
    }

    @Override
    protected String generateSparkCmd(KylinConfig config, String hadoopConf, String jars, String kylinJobJar,
            String appArgs) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "export HADOOP_CONF_DIR=%s && %s/bin/spark-submit --class io.kyligence.kap.engine.spark.application.SparkEntry ");

        appendSparkConf(sb, "spark.master", "local");
        appendSparkConf(sb, "spark.executor.extraClassPath", Paths.get(kylinJobJar).getFileName().toString());

        sb.append("--jars %s %s %s");
        String cmd = String.format(sb.toString(), hadoopConf, KylinConfig.getSparkHome(), jars, kylinJobJar, appArgs);
        logger.debug("spark submit cmd: {}", cmd);
        return cmd;
    }

}
