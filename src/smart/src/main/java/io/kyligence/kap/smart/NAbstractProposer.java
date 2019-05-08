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

package io.kyligence.kap.smart;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.smart.common.AccelerateInfo;

public abstract class NAbstractProposer {

    protected static Logger logger = LoggerFactory.getLogger(NAbstractProposer.class);

    final Map<String, AccelerateInfo> accelerateInfoMap;
    final NSmartContext smartContext;
    final KylinConfig kylinConfig;

    final String project;

    public NAbstractProposer(NSmartContext smartContext) {
        this.smartContext = smartContext;
        this.kylinConfig = smartContext.getKylinConfig();
        this.project = smartContext.getProject();

        this.accelerateInfoMap = smartContext.getAccelerateInfoMap();
    }

    void recordException(NSmartContext.NModelContext modelCtx, Exception e) {
        modelCtx.getModelTree().getOlapContexts().forEach(olapCtx -> {
            String sql = olapCtx.sql;
            final AccelerateInfo accelerateInfo = accelerateInfoMap.get(sql);
            Preconditions.checkNotNull(accelerateInfo);
            accelerateInfo.setFailedCause(e);
        });
    }

    abstract void propose();
}
