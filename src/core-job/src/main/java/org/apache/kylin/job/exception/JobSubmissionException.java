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

package org.apache.kylin.job.exception;

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.code.ErrorCodeProducer;

public class JobSubmissionException extends KylinException {

    private Map<String, KylinException> segmentFailInfos = new HashMap<>();

    public JobSubmissionException(String msg) {
        super(FAILED_CREATE_JOB, msg);
    }

    public JobSubmissionException(ErrorCodeProducer errorCodeProducer, Object... args) {
        super(errorCodeProducer, args);
    }

    public void addJobFailInfo(String segId, KylinException error) {
        segmentFailInfos.put(segId, error);
    }

    public Map<String, KylinException> getSegmentFailInfos() {
        return segmentFailInfos;
    }
}
