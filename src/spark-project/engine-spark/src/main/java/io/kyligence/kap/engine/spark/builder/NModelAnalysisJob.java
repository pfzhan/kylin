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

package io.kyligence.kap.engine.spark.builder;

import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.engine.spark.stats.analyzer.ModelAnalyzer;
import io.kyligence.kap.metadata.model.NDataModel;
import org.apache.kylin.common.util.OptionsHelper;

import java.util.Set;

public class NModelAnalysisJob extends NDataflowJob {

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        super.execute(optionsHelper);

        String dfName = optionsHelper.getOptionValue(OPTION_DATAFLOW_NAME);
        project = optionsHelper.getOptionValue(OPTION_PROJECT_NAME);
        Set<Integer> segmentIds = NSparkCubingUtil.str2Ints(optionsHelper.getOptionValue(OPTION_SEGMENT_IDS));

        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dfName);
        final NDataModel dataModel = dataflow.getModel();

        final ModelAnalyzer modelAnalyzer = new ModelAnalyzer(dataModel, config);
        logger.info("Start to analysis model {}", dataflow.getName());
        for (int segId : segmentIds) {
            NDataSegment seg = dataflow.getSegment(segId);
            logger.info("Analysis segment {}", seg.getName());
            modelAnalyzer.analyze(seg, ss);
        }
    }

    public static void main(String[] args) {
        NModelAnalysisJob modelAnalysisJob = new NModelAnalysisJob();
        modelAnalysisJob.execute(args);
    }
}
