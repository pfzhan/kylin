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
package io.kyligence.kap.streaming.app;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.JobTypeEnum;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.parser.AbstractDataParser;

public abstract class StreamingBuildApplication extends StreamingApplication {

    //params
    protected int durationSec;
    protected String watermark;
    protected String baseCheckpointLocation;

    protected String parserName;
    protected AbstractDataParser<ByteBuffer> dataParser;

    protected StreamingBuildApplication() {
        this.jobType = JobTypeEnum.STREAMING_BUILD;

        this.baseCheckpointLocation = kylinConfig.getStreamingBaseCheckpointLocation();
        Preconditions.checkState(StringUtils.isNotBlank(baseCheckpointLocation),
                "base checkpoint location must be configured, %s", baseCheckpointLocation);
    }

    @Override
    protected void prepareBeforeExecute() throws ExecuteException {
        super.prepareBeforeExecute();
        try {
            dataParser = AbstractDataParser.getDataParser(parserName, Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            throw new ExecuteException(e);
        }
    }

    public void parseParams(String[] args) {
        this.project = args[0];
        this.dataflowId = args[1];
        this.durationSec = Integer.parseInt(args[2]);
        this.watermark = args[3];
        this.distMetaUrl = args[4];
        this.parserName = args[5];
        this.jobId = StreamingUtils.getJobId(dataflowId, jobType.name());

        Preconditions.checkArgument(StringUtils.isNotEmpty(distMetaUrl), "distMetaUrl should not be empty!");
        Preconditions.checkArgument(StringUtils.isNotEmpty(parserName), "parserName should not be empty!");
    }

}
