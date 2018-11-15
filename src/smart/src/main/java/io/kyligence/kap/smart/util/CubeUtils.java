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

package io.kyligence.kap.smart.util;

import java.util.Collections;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.common.SmartConfig;

public class CubeUtils {

    private static final String BIG_INT = "bigint";

    private CubeUtils() {
    }

    public static FunctionDesc newFunctionDesc(NDataModel modelDesc, String expression, ParameterDesc param,
            String colDataType) {
        String returnType = FunctionDesc.proposeReturnType(expression, colDataType,
                Collections.singletonMap(FunctionDesc.FUNC_COUNT_DISTINCT,
                        SmartConfig.wrap(KylinConfig.getInstanceFromEnv()).getMeasureCountDistinctType()));
        FunctionDesc ret = FunctionDesc.newInstance(expression, param, returnType);
        ret.init(modelDesc);
        return ret;
    }

    public static FunctionDesc newCountStarFuncDesc(NDataModel modelDesc) {
        return newFunctionDesc(modelDesc, FunctionDesc.FUNC_COUNT, ParameterDesc.newInstance("1"), BIG_INT);
    }

    public static NDataModel.Measure newMeasure(FunctionDesc func, String name, int id) {
        NDataModel.Measure measure = new NDataModel.Measure();
        measure.setName(name);
        measure.setFunction(func);
        measure.id = id;
        return measure;
    }
}
