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

package org.apache.kylin.query.routing;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class CandidateSortTest {

    @Test
    public void testModelHintCandidateSort() {
        try (QueryContext queryContext = QueryContext.current()){
            {
                queryContext.setModelPriorities(new String[]{});
                val model1 = mockCandidate("model0001", "modelA", 1, 1);
                val model2 = mockCandidate("model0002", "modelB", 2, 2);
                sort(model1, model2).assertFirst(model1);
            }

            {
                queryContext.setModelPriorities(new String[]{"MODELB"});
                val model1 = mockCandidate("model0001", "modelA", 1, 1);
                val model2 = mockCandidate("model0002", "modelB", 2, 2);
                sort(model1, model2).assertFirst(model2);
            }

            {
                queryContext.setModelPriorities(new String[]{"MODELB", "MODELA"});
                val model1 = mockCandidate("model0001", "modelA", 1, 1);
                val model2 = mockCandidate("model0002", "modelB", 2, 2);
                sort(model1, model2).assertFirst(model2);
            }

            {
                queryContext.setModelPriorities(new String[]{"MODELC", "MODELA"});
                val model1 = mockCandidate("model0001", "modelA", 1, 1);
                val model2 = mockCandidate("model0002", "modelB", 2, 2);
                val model3 = mockCandidate("model0003", "modelC", 4, 4);
                sort(model1, model2, model3).assertFirst(model3);
            }
        }
    }

    @Test
    public void testSort() {
        {
            val model1 = mockCandidate("model0001", "modelA", 1, 1);
            val model2 = mockCandidate("model0002", "modelB", 2, 2);
            sort(model1, model2).assertFirst(model1);
        }

        {
            val model1 = mockCandidate("model0001", "modelA", 2, 1);
            val model2 = mockCandidate("model0002", "modelB", 2, 2);
            sort(model1, model2).assertFirst(model1);
        }

        {
            val model1 = mockCandidate("model0001", "modelA", 2, 2);
            val model2 = mockCandidate("model0002", "modelB", 2, 2);
            sort(model1, model2).assertFirst(model1);
        }

        {
            val model1 = mockCandidate("model0001", "modelA", 1, 1);
            val model2 = mockCandidate("model0002", "modelB", 2, 2);
            val model3 = mockCandidate("model0003", "modelC", 4, 4);
            sort(model1, model2, model3).assertFirst(model1);
        }
    }

    private interface SortedCandidate {

        void assertFirst(Candidate candidate);
    }

    private SortedCandidate sort(Candidate... candidates) {
        return candidate -> {
            Arrays.sort(candidates, Candidate.COMPARATOR);
            Assert.assertEquals(candidate.getRealization().getModel().getAlias(), candidates[0].getRealization().getModel().getAlias());
        };
    }

    private Candidate mockCandidate(String modelId, String modelName, int modelCost, double candidateCost) {
        val candidate = new Candidate();
        candidate.realization = mockRealization(modelId, modelName, modelCost);
        val cap = new CapabilityResult();
        cap.setSelectedCandidate(() -> candidateCost);
        candidate.setCapability(cap);
        return candidate;
    }

    private IRealization mockRealization(String modelId, String modelName, int cost) {
        return new IRealization() {
            @Override
            public CapabilityResult isCapable(SQLDigest digest, List<NDataSegment> prunedSegments) {
                return null;
            }

            @Override
            public CapabilityResult isCapable(SQLDigest digest, List<NDataSegment> prunedSegments, List<NDataSegment> prunedStreamingSegments) {
                return null;
            }

            @Override
            public String getType() {
                return null;
            }

            @Override
            public KylinConfig getConfig() {
                return null;
            }

            @Override
            public NDataModel getModel() {
                val model = new NDataModel();
                model.setAlias(modelName);
                model.setUuid(modelId);
                return model;
            }

            @Override
            public Set<TblColRef> getAllColumns() {
                return null;
            }

            @Override
            public Set<ColumnDesc> getAllColumnDescs() {
                return null;
            }

            @Override
            public List<TblColRef> getAllDimensions() {
                return null;
            }

            @Override
            public List<MeasureDesc> getMeasures() {
                return null;
            }

            @Override
            public List<IRealization> getRealizations() {
                return null;
            }

            @Override
            public FunctionDesc findAggrFunc(FunctionDesc aggrFunc) {
                return null;
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public String getUuid() {
                return null;
            }

            @Override
            public String getCanonicalName() {
                return null;
            }

            @Override
            public long getDateRangeStart() {
                return 0;
            }

            @Override
            public long getDateRangeEnd() {
                return 0;
            }

            @Override
            public int getCost() {
                return cost;
            }

            @Override
            public boolean hasPrecalculatedFields() {
                return false;
            }

            @Override
            public int getStorageType() {
                return 0;
            }

            @Override
            public boolean isStreaming() {
                return false;
            }
        };
    }

}
