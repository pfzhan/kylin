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

package io.kyligence.kap.streaming.metadata;

import io.kyligence.kap.engine.spark.job.BuildLayoutWithUpdate;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegDetails;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.streaming.request.LayoutUpdateRequest;
import io.kyligence.kap.streaming.rest.RestSupport;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BuildLayoutWithRestUpdate extends BuildLayoutWithUpdate {
    protected static final Logger logger = LoggerFactory.getLogger(BuildLayoutWithRestUpdate.class);

    protected void updateLayouts(KylinConfig config, String project, String dataflowId,
            final List<NDataLayout> layouts) {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        if (conf.isUTEnv()) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowUpdate update = new NDataflowUpdate(dataflowId);
                update.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
                NDataflowManager.getInstance(conf, project).updateDataflow(update);
                return 0;
            }, project);
        } else {
            callUpdateLayouts(conf, project, dataflowId, layouts);
        }
    }

    public static void callUpdateLayouts(KylinConfig conf, String project, String dataflowId, NDataLayout layouts) {
        callUpdateLayouts(conf, project, dataflowId, Arrays.asList(layouts));
    }

    public static void callUpdateLayouts(KylinConfig conf, String project, String dataflowId,
            final List<NDataLayout> layouts) {
        RestSupport rest = new RestSupport(conf);
        String url = "/streaming_jobs/dataflow/layout";
        List<NDataSegDetails> segDetails = layouts.stream().map(item -> item.getSegDetails())
                .collect(Collectors.toList());
        LayoutUpdateRequest req = new LayoutUpdateRequest(project, dataflowId, layouts, segDetails);
        try {
            rest.execute(rest.createHttpPut(url), req);
        } finally {
            rest.close();
        }
        StreamingUtils.replayAuditlog();
    }
}
