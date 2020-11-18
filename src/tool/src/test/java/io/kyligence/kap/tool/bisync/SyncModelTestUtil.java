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

package io.kyligence.kap.tool.bisync;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import org.apache.kylin.common.KylinConfig;

public class SyncModelTestUtil {

    public static SyncContext createSyncContext(String project, String modelId, KylinConfig kylinConfig) {
        SyncContext syncContext = new SyncContext();
        syncContext.setProjectName(project);
        syncContext.setModelId(modelId);
        syncContext.setTargetBI(SyncContext.BI.TABLEAU_CONNECTOR_TDS);
        syncContext.setModelElement(SyncContext.ModelElement.AGG_INDEX_COL);
        syncContext.setHost("localhost");
        syncContext.setPort(7070);
        syncContext.setDataflow(NDataflowManager.getInstance(kylinConfig, project).getDataflow(modelId));
        syncContext.setKylinConfig(kylinConfig);
        return syncContext;
    }
}
