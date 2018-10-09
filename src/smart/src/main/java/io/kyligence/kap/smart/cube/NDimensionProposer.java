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

package io.kyligence.kap.smart.cube;

import java.util.Map;

import org.apache.kylin.dimension.DateDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.FixedLenDimEnc;
import org.apache.kylin.dimension.IntegerDimEnc;
import org.apache.kylin.dimension.TimeDimEnc;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NEncodingDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.common.SmartConfig;

public class NDimensionProposer extends NAbstractCubeProposer {
    NDimensionProposer(NSmartContext.NModelContext context) {
        super(context);
    }

    @Override
    void doPropose(NCubePlan cubePlan) {
        Map<Integer, NEncodingDesc> encs = Maps.newTreeMap();

        // keep old dimensions
        encs.putAll(cubePlan.getCubePlanOverrideEncodings());

        NDataModel model = context.getTargetModel();
        for (Map.Entry<Integer, TblColRef> colEntry : model.getEffectiveColsMap().entrySet()) {
            if (encs.containsKey(colEntry.getKey()))
                continue; // TODO: for those existing dimension but not used in any ready cuboids, we still can modify the encoding.

            encs.put(colEntry.getKey(), suggestEncoding(colEntry.getValue()));
        }

        cubePlan.setCubePlanOverrideEncodings(encs);
    }

    private NEncodingDesc suggestEncoding(TblColRef colRef) {
        // select encoding according to column type
        // eg. date, tinyint, integer, smallint, bigint
        // TODO: we can set boolean encoding according to column type and cardinality, but cardinality is not precise.
        DataType dataType = colRef.getType();

        // datatime family
        if (dataType.isDate()) {
            return newEnc(DateDimEnc.ENCODING_NAME);
        } else if (dataType.isDateTimeFamily()) {
            return newEnc(TimeDimEnc.ENCODING_NAME);
        }

        // number family
        if (dataType.isTinyInt()) {
            return newEnc(String.format("%s:%d", IntegerDimEnc.ENCODING_NAME, 1));
        } else if (dataType.isSmallInt()) {
            return newEnc(String.format("%s:%d", IntegerDimEnc.ENCODING_NAME, 2));
        } else if (dataType.isInt()) {
            return newEnc(String.format("%s:%d", IntegerDimEnc.ENCODING_NAME, 4));
        } else if (dataType.isIntegerFamily()) {
            return newEnc(String.format("%s:%d", IntegerDimEnc.ENCODING_NAME, 8));
        } else if (dataType.isNumberFamily()) {
            return newEnc(DictionaryDimEnc.ENCODING_NAME);
        }

        // select dict or fixlen for other type columns according to cardinality
        SmartConfig smartConfig = context.getSmartContext().getSmartConfig();
        TableExtDesc.ColumnStats columnStats = context.getSmartContext().getColumnStats(colRef);
        if (columnStats != null) {
            long cardinality = columnStats.getCardinality();

            if (cardinality > smartConfig.getRowkeyDictEncCardinalityMax())
                return newEnc(String.format("%s:%d", FixedLenDimEnc.ENCODING_NAME, Math.min(
                        columnStats.getMaxLengthValue().getBytes().length, smartConfig.getRowkeyFixLenLengthMax())));
            else
                return newEnc(DictionaryDimEnc.ENCODING_NAME);
        }

        String defaultEnc = smartConfig.getRowkeyDefaultEnc();
        return newEnc(defaultEnc);
    }

    private NEncodingDesc newEnc(String name) {
        return newEnc(name, 1);
    }

    private NEncodingDesc newEnc(String name, int ver) {
        NEncodingDesc encodingDesc = new NEncodingDesc();
        encodingDesc.setName(name);
        encodingDesc.setVersion(ver);
        return encodingDesc;
    }
}
