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

package io.kyligence.kap.metadata.model.util;

import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;

import io.kyligence.kap.metadata.model.NDataModel;

public class MeasureUtil {
    public static final String MEASURE_NAME_PREFIX = "MEASURE_AUTO_";

    public static int getBiggestMeasureIndex(NDataModel dataModel) {
        int biggest = 0;
        for (String name : dataModel.getAllMeasures().stream().filter(m -> !m.isTomb()).map(MeasureDesc::getName)
                .collect(Collectors.toList())) {
            if (name.startsWith(MEASURE_NAME_PREFIX)) {
                String idxStr = name.substring(MEASURE_NAME_PREFIX.length());
                int idx;
                try {
                    idx = Integer.parseInt(idxStr);
                } catch (NumberFormatException e) {
                    break;
                }
                if (idx > biggest) {
                    biggest = idx;
                }
            }
        }
        return biggest;
    }

    public static boolean equals(MeasureDesc m1, MeasureDesc m2) {
        if (m1 == null) {
            return m2 == null;
        }
        if (m2 == null) {
            return false;
        }
        FunctionDesc f1 = m1.getFunction();
        FunctionDesc f2 = m2.getFunction();
        if (!f1.getExpression().equals(f2.getExpression())) {
            return false;
        }

        if (f1.getParameters().size() != f2.getParameters().size()) {
            return false;
        }

        for (int i = 0; i < f1.getParameters().size(); i++) {
            ParameterDesc p1 = f1.getParameters().get(i);
            ParameterDesc p2 = f2.getParameters().get(i);
            if (!p1.getType().equals(p2.getValue()) || !p1.getValue().equals(p2.getValue())) {
                return false;
            }
        }
        return true;
    }
}
