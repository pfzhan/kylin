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

package io.kyligence.kap.cube.model;

import java.util.Map;

import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;

public class NDimensionProposer extends NAbstractCubeProposer {
    NDimensionProposer(NSmartContext.NModelContext context) {
        super(context);
    }

    @Override
    void doPropose(NCubePlan cubePlan) {
        Map<Integer, NDimensionDesc> dimDescMap = Maps.newHashMap();

        // keep old dimensions
        for (NDimensionDesc dimensionDesc : cubePlan.getDimensions()) {
            dimDescMap.put(dimensionDesc.getId(), dimensionDesc);
        }

        NDataModel model = context.getTargetModel();
        for (Map.Entry<Integer, TblColRef> colEntry : model.getEffectiveColsMap().entrySet()) {
            if (dimDescMap.containsKey(colEntry.getKey()))
                continue;

            NDimensionDesc dimDesc = new NDimensionDesc();
            dimDesc.setId(colEntry.getKey());

            NDimensionDesc.NEncodingDesc encodingDesc = new NDimensionDesc.NEncodingDesc();
            encodingDesc.setName("dict"); // TODO: will decide encoding when we have table stats
            encodingDesc.setVersion(1);
            dimDesc.setEncoding(encodingDesc);

            dimDescMap.put(colEntry.getKey(), dimDesc);
        }
        cubePlan.setDimensions(Lists.newArrayList(dimDescMap.values()));
    }
}
