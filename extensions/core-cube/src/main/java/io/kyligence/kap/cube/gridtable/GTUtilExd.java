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

package io.kyligence.kap.cube.gridtable;

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTUtil;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.datatype.DateTimeSerializer;
import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import io.kyligence.kap.metadata.filter.EvaluatableFunctionTupleFilter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.raw.RawColumnCodec;
import io.kyligence.kap.metadata.filter.TupleFilterSerializerExt;

public class GTUtilExd extends GTUtil {

    static public TblColRef getRealColFromMockUp(TblColRef mockUpCol, Cuboid cuboid) {
        return cuboid.getColumns().get(mockUpCol.getColumnDesc().getZeroBasedIndex());
    }

    public static TupleFilter kapConvertFilterColumnsAndConstants(TupleFilter rootFilter, GTInfo info, //
            List<TblColRef> colMapping, Set<TblColRef> unevaluatableColumnCollector) {
        return kapConvertFilter(rootFilter, info, colMapping, true, unevaluatableColumnCollector);
    }

    // converts TblColRef to GridTable column, encode constants, drop unEvaluatable parts
    private static TupleFilter kapConvertFilter(TupleFilter rootFilter, final GTInfo info, //
            final List<TblColRef> colMapping, final boolean encodeConstants, //
            final Set<TblColRef> unevaluatableColumnCollector) {

        IFilterCodeSystem<ByteArray> filterCodeSystem = wrap(info.getCodeSystem().getComparator());

        byte[] bytes = TupleFilterSerializerExt.serialize(rootFilter, new KapGTConvertDecorator(unevaluatableColumnCollector, colMapping, info, encodeConstants), filterCodeSystem);

        return TupleFilterSerializerExt.deserialize(bytes, filterCodeSystem);
    }

    private static class KapGTConvertDecorator extends GTConvertDecorator {
        public KapGTConvertDecorator(Set<TblColRef> unevaluatableColumnCollector, List<TblColRef> colMapping, GTInfo info, boolean encodeConstants) {
            super(unevaluatableColumnCollector, colMapping, info, encodeConstants);
        }

        @Override
        public TupleFilter onSerialize(TupleFilter filter) {
            if (filter == null)
                return null;

            //TODO: because whether compareTupleFilter is evaluatable is undetermined, ignore all not() conditions
            if (filter.getOperator() == TupleFilter.FilterOperatorEnum.NOT) {
                TupleFilter.collectColumns(filter, unevaluatableColumnCollector);
                return ConstantTupleFilter.TRUE;
            }

            // shortcut for unEvaluatable filter
            if (!filter.isEvaluable() || !isFilterEvaluatable(filter)) {
                TupleFilter.collectColumns(filter, unevaluatableColumnCollector);
                return ConstantTupleFilter.TRUE;
            }

            // map to column onto grid table
            if (colMapping != null && filter instanceof ColumnTupleFilter) {
                ColumnTupleFilter colFilter = (ColumnTupleFilter) filter;
                int gtColIdx = colMapping.indexOf(colFilter.getColumn());
                return new ColumnTupleFilter(info.colRef(gtColIdx));
            }

            // encode constants
            if (encodeConstants && filter instanceof CompareTupleFilter) {
                return encodeConstants((CompareTupleFilter) filter);
            }
            if (encodeConstants && filter instanceof EvaluatableFunctionTupleFilter) {
                return encodeConstants((EvaluatableFunctionTupleFilter) filter);
            }

            return filter;
        }

        private boolean isFilterEvaluatable(TupleFilter filter) {
            if (!(filter instanceof CompareTupleFilter)) {
                return true;
            }
            CompareTupleFilter compareTupleFilter = (CompareTupleFilter) filter;
            TupleFilter.FilterOperatorEnum operatorEnum = compareTupleFilter.getOperator();
            if (operatorEnum == TupleFilter.FilterOperatorEnum.GT || operatorEnum == TupleFilter.FilterOperatorEnum.GTE || //
                    operatorEnum == TupleFilter.FilterOperatorEnum.LT || operatorEnum == TupleFilter.FilterOperatorEnum.LTE) {
                //TODO: due to https://github.com/Kyligence/KAP/issues/96, < and > are restricted
                DataTypeSerializer<?> dataTypeSerializer = RawColumnCodec.createSerializer(compareTupleFilter.getColumn().getType());
                if (dataTypeSerializer instanceof DateTimeSerializer) {
                    return true;
                } else {
                    return false;
                }
            }

            return true;
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        protected TupleFilter encodeConstants(EvaluatableFunctionTupleFilter funcFilter) {

            if (!funcFilter.isLikeFunction()) {
                return funcFilter;
            }

            ConstantTupleFilter constantTupleFilter = funcFilter.getConstantTupleFilter();
            if (constantTupleFilter == null || constantTupleFilter.getValues() == null || constantTupleFilter.getValues().isEmpty()) {
                return funcFilter;
            }

            BuiltInFunctionTupleFilter newFuncFilter;
            newFuncFilter = new EvaluatableFunctionTupleFilter(funcFilter.getName());
            newFuncFilter.addChild(funcFilter.getColumnContainerFilter());

            TblColRef externalCol = funcFilter.getColumn();
            int col = colMapping == null ? externalCol.getColumnDesc().getZeroBasedIndex() : colMapping.indexOf(externalCol);

            ByteArray code;

            // translate constant into code
            Set newValues = Sets.newHashSet();
            for (Object value : constantTupleFilter.getValues()) {
                code = translate(col, value, 0);
                if (code == null) {
                    throw new IllegalStateException("Cannot serialize BuiltInFunctionTupleFilter");
                }
                newValues.add(code);
            }
            newFuncFilter.addChild(new ConstantTupleFilter(newValues));

            return newFuncFilter;
        }

    }

}
