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

package io.kyligence.kap.job.execution;

import org.apache.kylin.common.util.TimeUtil;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.rest.delegate.JobMetadataInvoker;
import lombok.val;

public class DefaultChainedExecutableOnTable extends DefaultChainedExecutable {

    public DefaultChainedExecutableOnTable() {
        super();
    }

    public DefaultChainedExecutableOnTable(Object notSetId) {
        super(notSetId);
    }

    public String getTableIdentity() {
        return getParam(NBatchConstants.P_TABLE_NAME);
    }

    @Override
    public String getTargetSubjectAlias() {
        return getTableIdentity();
    }

    @Override
    protected void afterUpdateOutput(String jobId) {
        val job = getExecutableManager(getProject()).getJob(jobId);
        long duration = job.getDuration();
        long endTime = job.getEndTime();
        long startOfDay = TimeUtil.getDayStart(endTime);
        JobMetadataInvoker.getInstance().updateStatistics(project, startOfDay, null, duration, 0, 0);
    }
}
