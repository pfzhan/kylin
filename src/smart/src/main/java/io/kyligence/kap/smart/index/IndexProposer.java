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

package io.kyligence.kap.smart.index;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.AbstractContext;

class IndexProposer extends AbstractIndexProposer {

    IndexProposer(AbstractContext.ModelContext context) {
        super(context);
    }

    @Override
    public IndexPlan execute(IndexPlan indexPlan) {

        Map<IndexEntity.IndexIdentifier, IndexEntity> indexEntityMap = indexPlan.getAllIndexesMap();

        IndexSuggester suggester = new IndexSuggester(context, indexPlan, indexEntityMap);
        suggester.suggestIndexes(context.getModelTree());

        indexPlan.setIndexes(Lists.newArrayList(indexEntityMap.values()));
        initModel(context.getTargetModel());
        return indexPlan;
    }

    // Important fix for KE-14714: all columns in table index will be treat as dimension.
    // Extra dimensions may add to model in IndexSuggester.suggestTableIndexDimensions,
    // code line: namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION).
    private void initModel(NDataModel modelDesc) {
        String project = context.getProposeContext().getProject();
        modelDesc.init(KylinConfig.getInstanceFromEnv(), project, Lists.newArrayList());
    }
}