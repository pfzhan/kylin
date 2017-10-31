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

package io.kyligence.kap.metadata.model;

import java.io.IOException;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.MeasureDesc;

public class NDataModelManager extends DataModelManager {
    public NDataModelManager(KylinConfig config) throws IOException {
        super(config);
    }

    /**
     * if there is some change need be applied after getting a cubeDesc from front-end, do it here
     * @param dataModel
     */
    private void postProcessKapModel(NDataModel dataModel) {
        for (Map.Entry<Integer, NDataModel.Measure> measureEntry : dataModel.getEffectiveMeasureMap().entrySet()) {
            MeasureDesc measureDesc = measureEntry.getValue();
            if (TopNMeasureType.FUNC_TOP_N.equalsIgnoreCase(measureDesc.getFunction().getExpression())) {
                TopNMeasureType.fixMeasureReturnType(measureDesc);
            }
        }
    }
}
