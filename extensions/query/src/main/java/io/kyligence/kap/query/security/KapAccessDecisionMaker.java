package io.kyligence.kap.query.security;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPAuthentication;
import org.apache.kylin.query.relnode.OLAPContext;

public class KapAccessDecisionMaker implements OLAPContext.IAccessController {
    private static final IACLMetaData metaSource = new KapAclReader();

    public TupleFilter rowFilter(IRealization realization, OLAPAuthentication olapAuthentication) {

        if (null == realization || !(realization instanceof CubeInstance))
            return null;
        Properties properties = ((KylinConfigExt) ((CubeInstance) realization).getConfig()).getAllProperties();
        CubeDesc cubeDesc = ((CubeInstance) realization).getDescriptor();
        if (null == properties && properties.isEmpty())
            return null;
        IRowFilter iRowFilter = new KapUsernameRowFilter();
        return iRowFilter.getRowFilter(olapAuthentication, cubeDesc.listAllColumns(), cubeDesc.getOverrideKylinProps());
    }

    public void columnFilter(Collection<TblColRef> columns, OLAPAuthentication olapAuthentication) {
        ACLAttribution userAcl = IACLMetaData.limitedColumnsByUser;
        for (Map.Entry<String, String> attr : userAcl.get(olapAuthentication.getUsername().toLowerCase()).entrySet()) {
            for (TblColRef tblColRef : columns) {
                String columnName = tblColRef.getTable() + "." + tblColRef.getName();
                if (columnName.toLowerCase().contains(attr.getKey()) && attr.getValue().equals(IACLMetaData.DENY)) {
                    throw new IllegalArgumentException("Current User: " + olapAuthentication.getUsername() + " has no rights to access column: " + columnName);
                }
            }
        }
    }

    @Override
    public TupleFilter check(OLAPAuthentication olapAuthentication, Collection<TblColRef> columns, IRealization realization) throws IllegalArgumentException {
        if (null == olapAuthentication.getUsername())
            return null;
        if (IACLMetaData.limitedColumnsByUser.isEmpty())
            return null;

        Collection<TblColRef> requiredColumns;
        if (0 == columns.size()) {
            requiredColumns = realization.getAllColumns();
        } else {
            requiredColumns = columns;
        }
        columnFilter(requiredColumns, olapAuthentication);
        TupleFilter tupleFilter = rowFilter(realization, olapAuthentication);
        return tupleFilter;
    }
}
