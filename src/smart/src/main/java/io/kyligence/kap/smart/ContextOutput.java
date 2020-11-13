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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.springframework.beans.BeanUtils;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.DimensionRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.MeasureRecItemV2;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.Data;
import lombok.val;

@Data
public class ContextOutput implements Serializable {

    public static void merge(AbstractContext targetContext, ContextOutput output) {
        BeanUtils.copyProperties(output, targetContext);
        for (val modelContextOutput : output.getModelContextOutputs()) {
            val modelContext = new AbstractContext.ModelContext(targetContext, null);
            BeanUtils.copyProperties(modelContextOutput, modelContext);
            targetContext.getModelContexts().add(modelContext);
        }
    }

    public static ContextOutput from(AbstractContext sourceContext) {
        val output = new ContextOutput();
        BeanUtils.copyProperties(sourceContext, output);
        for (AbstractContext.ModelContext modelContext : sourceContext.getModelContexts()) {
            val modelOutput = new ModelContextOutput();
            BeanUtils.copyProperties(modelContext, modelOutput);
            output.getModelContextOutputs().add(modelOutput);
        }
        return output;
    }

    private Map<String, AccelerateInfo> accelerateInfoMap = Maps.newHashMap();

    private List<ModelContextOutput> modelContextOutputs = Lists.newArrayList();

    @Data
    public static class ModelContextOutput implements Serializable {
        private NDataModel targetModel; // output model
        private NDataModel originModel; // used when update existing models

        private IndexPlan targetIndexPlan;
        private IndexPlan originIndexPlan;

        private Map<String, CCRecItemV2> ccRecItemMap = Maps.newHashMap();
        private Map<String, DimensionRecItemV2> dimensionRecItemMap = Maps.newHashMap();
        private Map<String, MeasureRecItemV2> measureRecItemMap = Maps.newHashMap();
        private Map<String, LayoutRecItemV2> indexRexItemMap = Maps.newHashMap();

        private boolean snapshotSelected;

        private Map<String, ComputedColumnDesc> usedCC = Maps.newHashMap();
    }
}
