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
package io.kyligence.kap.rest.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import lombok.Data;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.exception.JobSubmissionException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Data
public class JobInfoResponseWithFailure extends JobInfoResponse {

    @JsonProperty("failed_segments")
    List<FailedSegmentJobWithReason> failedSegments = new LinkedList<>();

    public void addFailedSeg(NDataflow dataflow, JobSubmissionException jobSubmissionException) {
        for (Map.Entry<String, KylinException> entry : jobSubmissionException.getSegmentFailInfos().entrySet()) {
            String segId  = entry.getKey();
            KylinException error = entry.getValue();

            FailedSegmentJobWithReason failedSeg = new FailedSegmentJobWithReason(dataflow, dataflow.getSegment(segId));
            failedSeg.setErrorCode(error.getErrorCode());
            failedSeg.setErrorMsg(error.getMessage());
            failedSegments.add(failedSeg);
        }
    }

    @Data
    public static class FailedSegmentJobWithReason extends NDataSegmentResponse {

        public FailedSegmentJobWithReason(NDataflow dataflow, NDataSegment segment) {
            super(dataflow, segment);
        }

        @JsonProperty("error_code")
        private ErrorCode errorCode;

        @JsonProperty("error_msg")
        private String errorMsg;

    }

}
