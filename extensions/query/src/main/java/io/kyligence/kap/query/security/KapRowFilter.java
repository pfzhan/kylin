package io.kyligence.kap.query.security;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;

import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPAuthentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KapRowFilter implements IRowFilter {
    private static final Logger logger = LoggerFactory.getLogger(KapRowFilter.class);

    private Hashtable<TblColRef, String> cubeAccessControlColumns = new Hashtable<>();

    @Override
    public TupleFilter getRowFilter(OLAPAuthentication authentication, Collection<TblColRef> allCubeColumns, Map<String, String> conditions) {
        Hashtable<String, String> aclColumns = IACLMetaData.accessControlColumnsByUser.get(authentication.getUsername().toLowerCase());

        fetchCubeLimitColumns(aclColumns, allCubeColumns);

        if (0 == cubeAccessControlColumns.size()) {
            logger.error("There are no access control columns on current cube");
            return null;
        }

        TupleFilter finalFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        for (Map.Entry<TblColRef, String> condition : cubeAccessControlColumns.entrySet()) {
            TupleFilter equalFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
            TupleFilter colFilter = new ColumnTupleFilter(condition.getKey());
            TupleFilter constantFilter = new ConstantTupleFilter(condition.getValue());
            equalFilter.addChild(colFilter);
            equalFilter.addChild(constantFilter);
            finalFilter.addChild(equalFilter);
        }
        return finalFilter;
    }

    private void fetchCubeLimitColumns(Hashtable<String, String> aclColumns, Collection<TblColRef> allCubeColumns) {
        for (TblColRef cubeCol : allCubeColumns) {
            for (Map.Entry<String, String> aclCol : aclColumns.entrySet()) {
                if (cubeCol.getCanonicalName().toLowerCase().contains(aclCol.getKey()) & !isBooleanValue(aclCol.getValue())) {
                    cubeAccessControlColumns.put(cubeCol, aclCol.getValue());
                }
            }
        }
    }

    private boolean isBooleanValue(String value) {
        return (value.equalsIgnoreCase(IACLMetaData.DENY) || value.equalsIgnoreCase(IACLMetaData.YES));
    }
}
