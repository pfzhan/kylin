package io.kyligence.kap.query.security;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
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

    private List<TblColRef> tblColumns = new ArrayList<>();

    @Override
    public TupleFilter getRowFilter(OLAPAuthentication authentication, Collection<TblColRef> allCubeColumns, Map<String, String> conditions) {

        List<String> columns = new ArrayList<>();

        for (Map.Entry<String, String> entry : conditions.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(IRowFilter.ACL_COLUMN))
                columns.add(entry.getValue());
        }
        // validate if the given username column is contained by cube
        for (String col : columns) {
            for (TblColRef tblColRef : allCubeColumns) {
                String columnName = tblColRef.getTable() + "." + tblColRef.getName();
                if (columnName.toLowerCase().contains(col)) {
                    tblColumns.add(tblColRef);
                }
            }
        }
        if (0 == tblColumns.size()) {
            logger.error("the given acl_columns are not contained in current cube");
            return null;
        }

        Hashtable<String, String> attrs = IACLMetaData.limitedColumnsByUser.get(authentication.getUsername().toLowerCase());

        String value = authentication.getUsername();

        TupleFilter finalFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        TupleFilter rootFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        TupleFilter colFilter = new ColumnTupleFilter(tblColumns.get(0));
        TupleFilter constantFilter = new ConstantTupleFilter(value);
        rootFilter.addChild(colFilter);
        rootFilter.addChild(constantFilter);
        finalFilter.addChild(rootFilter);

        return finalFilter;
    }
}
