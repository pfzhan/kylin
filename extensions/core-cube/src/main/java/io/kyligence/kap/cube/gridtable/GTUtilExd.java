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
import org.apache.kylin.metadata.filter.EvaluatableFunctionTupleFilter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Sets;

import io.kyligence.kap.raw.RawDecoder;

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

        byte[] bytes = TupleFilterSerializer.serialize(rootFilter, new KapGTConvertDecorator(unevaluatableColumnCollector, colMapping, info, encodeConstants), filterCodeSystem);

        return TupleFilterSerializer.deserialize(bytes, filterCodeSystem);
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
                // due to https://github.com/Kyligence/KAP/issues/96, < and > are restricted
                DataTypeSerializer<?> dataTypeSerializer = RawDecoder.getSerializer(compareTupleFilter.getColumn().getType());
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
