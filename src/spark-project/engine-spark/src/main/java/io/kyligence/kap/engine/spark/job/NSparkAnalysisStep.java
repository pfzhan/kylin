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

package io.kyligence.kap.engine.spark.job;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;

import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.builder.NModelAnalysisJob;
import io.kyligence.kap.engine.spark.merger.MetadataMerger;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class NSparkAnalysisStep extends NSparkExecutable {

    public NSparkAnalysisStep() {
        this.setSparkSubmitClassName(NModelAnalysisJob.class.getName());
        this.setName(ExecutableConstants.STEP_NAME_DATA_PROFILING);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        final Set<String> dumpList = Sets.newHashSet();
        NDataflow df = NDataflowManager.getInstance(config, getProject()).getDataflow(getDataflowId());
        dumpList.addAll(df.collectPrecalculationResource());

        // dump table ext
        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, getProject());
        for (final TableRef tableRef : df.getModel().getAllTables()) {
            final TableExtDesc tableExtDesc = tableMetadataManager.getTableExtIfExists(tableRef.getTableDesc());
            if (tableExtDesc == null) {
                continue;
            }
            dumpList.add(tableExtDesc.getResourcePath());
        }
        return dumpList;
    }

    @Override
    public boolean needMergeMetadata() {
        return true;
    }

    @Override
    public void mergerMetadata(MetadataMerger merger) {
        merger.mergeAnalysis(this);
    }
}
