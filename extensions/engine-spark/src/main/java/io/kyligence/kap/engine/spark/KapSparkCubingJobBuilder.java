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

package io.kyligence.kap.engine.spark;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.spark.SparkBatchCubingJobBuilder2;
import org.apache.kylin.engine.spark.SparkExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.mr.KapBatchCubingJobBuilder;

public class KapSparkCubingJobBuilder extends KapBatchCubingJobBuilder {
    private static final Logger logger = LoggerFactory.getLogger(KapSparkCubingJobBuilder.class);

    public KapSparkCubingJobBuilder(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
    }

    @Override
    protected void addLayerCubingSteps(final CubingJob result, final String jobId, final String cuboidRootPath) {
        logger.info("Configure Spark cubing job");
        final SparkExecutable sparkExecutable = new SparkExecutable();
        sparkExecutable.setClassName(KapSparkCubingByLayer.class.getName());
        SparkBatchCubingJobBuilder2.configureSparkJob(seg, sparkExecutable, jobId, cuboidRootPath);
        result.addTask(sparkExecutable);
        result.addTask(createUpdateLayerOutputDirStep(cuboidRootPath, jobId));
    }

    @Override
    protected void addInmemCubingSteps(final CubingJob result, String jobId, String cuboidRootPath) {

    }
}
