/**
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

package io.kyligence.kap.measure.percentile;

import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

public class PercentileMeasureType extends MeasureType<PercentileCounter> {
    // compression ratio saved in DataType.precision
    private final DataType dataType;

    public PercentileMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public MeasureIngester<PercentileCounter> newIngester() {
        return new MeasureIngester<PercentileCounter>() {
            PercentileCounter current = new PercentileCounter(dataType.getPrecision());

            @Override
            public PercentileCounter valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                PercentileCounter counter = current;
                counter.clear();
                for (String v : values) {
                    if (v != null)
                        counter.add(Double.parseDouble(v));
                }
                return counter;
            }
        };
    }

    @Override
    public MeasureAggregator<PercentileCounter> newAggregator() {
        return new PercentileAggregator(dataType.getPrecision());
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        return PercentileAggFunc.class;
    }
}
