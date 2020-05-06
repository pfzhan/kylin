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

package io.kyligence.kap.smart.model;

import java.util.List;

import org.apache.kylin.metadata.model.ParameterDesc;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.AbstractContext;

public class NShrinkComputedColumnProposer extends NAbstractModelProposer {

    public NShrinkComputedColumnProposer(AbstractContext.NModelContext modelCtx) {
        super(modelCtx);
    }

    @Override
    protected void execute(NDataModel modelDesc) {
        discardCCFieldIfNotApply(modelDesc);
    }

    private void discardCCFieldIfNotApply(NDataModel model) {
        List<ComputedColumnDesc> originCC = Lists.newArrayList(model.getComputedColumnDescs());
        originCC.removeIf(computedColumnDesc -> !isCCUsedByModel(computedColumnDesc, model));
        model.setComputedColumnDescs(originCC);
    }

    private boolean isCCUsedByModel(ComputedColumnDesc cc, NDataModel model) {
        // check dimension
        for (NDataModel.NamedColumn column : model.getAllNamedColumns()) {
            if (column.isExist() && column.getAliasDotColumn().equals(cc.getFullName())) {
                return true;
            }
        }

        // check measure
        for (NDataModel.Measure allMeasure : model.getAllMeasures()) {
            for (ParameterDesc parameter : allMeasure.getFunction().getParameters()) {
                if (parameter.isConstantParameterDesc())
                    continue;

                if (parameter.getColRef().getColumnDesc().isComputedColumn()
                        && parameter.getColRef().getColumnDesc().getName().equals(cc.getColumnName())) {
                    return true;
                }
            }
        }
        return false;
    }
}
