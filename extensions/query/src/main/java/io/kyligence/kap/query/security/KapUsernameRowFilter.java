package io.kyligence.kap.query.security;

import java.util.Collection;
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

public class KapUsernameRowFilter implements IRowFilter {
    private static final Logger logger = LoggerFactory.getLogger(KapUsernameRowFilter.class);

    @Override
    public TupleFilter getRowFilter(OLAPAuthentication authentication, Collection<TblColRef> allCubeColumns, Map<String, String> conditions) {

        String userColumn = conditions.get(IRowFilter.USER_COLUMN);
        if (null == userColumn || userColumn.isEmpty())
            return null;

        // validate if the given username column is contained by cube
        TblColRef cubeUserColumn = null;
        for (TblColRef tblColRef : allCubeColumns) {
            String columnName = tblColRef.getTable() + "." + tblColRef.getName();
            if (columnName.contains(userColumn)) {
                cubeUserColumn = tblColRef;
                break;
            }
        }
        if (null == cubeUserColumn) {
            logger.error("the given column of username is not contained in current cube");
            return null;
        }
        // build the "And USER = XXX" condition filter
        TupleFilter combineFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        TupleFilter rootFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        TupleFilter colFilter = new ColumnTupleFilter(cubeUserColumn);
        TupleFilter constantFilter = new ConstantTupleFilter(authentication.getUsername());
        rootFilter.addChild(colFilter);
        rootFilter.addChild(constantFilter);
        combineFilter.addChild(rootFilter);
        return combineFilter;
    }
}
