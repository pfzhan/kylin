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

package io.kyligence.kap.job.execution.handler;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.base.Preconditions;

import io.kyligence.kap.job.execution.DefaultChainedExecutableOnModel;
import lombok.val;

public class ExecutableHandlerFactory {

    public static ExecutableHandler createExecutableHandler(DefaultChainedExecutableOnModel job) {
        val segmentIds = job.getTargetSegments();
        switch (job.getJobType()) {
        case INDEX_BUILD:
        case SUB_PARTITION_BUILD:
            return new ExecutableAddCuboidHandler(job);
        case INC_BUILD:
            Preconditions.checkArgument(CollectionUtils.isNotEmpty(segmentIds));
            return new ExecutableAddSegmentHandler(job.getProject(), job.getTargetSubject(), job.getSubmitter(),
                    segmentIds.get(0), job.getId());
        case INDEX_MERGE:
        case INDEX_REFRESH:
        case SUB_PARTITION_REFRESH:
            Preconditions.checkArgument(CollectionUtils.isNotEmpty(segmentIds));
            return new ExecutableMergeOrRefreshHandler(job.getProject(), job.getTargetSubject(), job.getSubmitter(),
                    segmentIds.get(0), job.getId());
        default:
            throw new IllegalArgumentException("illegal job type");
        }
    }
}
