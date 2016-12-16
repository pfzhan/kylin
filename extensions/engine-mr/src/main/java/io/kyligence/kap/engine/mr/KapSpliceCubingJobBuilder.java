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

package io.kyligence.kap.engine.mr;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;

import io.kyligence.kap.engine.mr.steps.KapSpliceBaseCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapSpliceInMemCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapSpliceNDCuboidJob;

public class KapSpliceCubingJobBuilder extends KapBatchCubingJobBuilder {

    public KapSpliceCubingJobBuilder(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
    }

    protected Class<? extends AbstractHadoopJob> getInMemCuboidJob() {
        return KapSpliceInMemCuboidJob.class;
    }

    protected Class<? extends AbstractHadoopJob> getNDCuboidJob() {
        return KapSpliceNDCuboidJob.class;
    }

    protected Class<? extends AbstractHadoopJob> getBaseCuboidJob() {
        return KapSpliceBaseCuboidJob.class;
    }
}
