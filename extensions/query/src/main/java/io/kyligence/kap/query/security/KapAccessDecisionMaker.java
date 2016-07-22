package io.kyligence.kap.query.security;

import java.util.Collection;
import java.util.Properties;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPAuthentication;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.storage.hybrid.HybridInstance;

public class KapAccessDecisionMaker implements OLAPContext.IAccessController {
    private static final IACLMetaData metaSource = new KapAclReader();

    public TupleFilter rowFilter(IRealization realization, OLAPAuthentication olapAuthentication) {
        // only process CubeInstance
        if (null == realization || !(realization instanceof CubeInstance))
            return null;
        Properties properties = ((KylinConfigExt) ((CubeInstance) realization).getConfig()).getAllProperties();
        CubeDesc cubeDesc = ((CubeInstance) realization).getDescriptor();
        if (null == properties && properties.isEmpty())
            return null;
        IRowFilter iRowFilter = (IRowFilter) ClassUtil.newInstance(properties.getProperty(IRowFilter.ROW_FILTER));
        return iRowFilter.getRowFilter(olapAuthentication, cubeDesc.listAllColumns(), cubeDesc.getOverrideKylinProps());
    }

    public void columnFilter(Collection<TblColRef> columns, OLAPAuthentication olapAuthentication) {
        for (String limitedColumn : IACLMetaData.limitedColumnsByUser.get(olapAuthentication.getUsername().toLowerCase())) {
            for (TblColRef tblColRef : columns) {
                String columnName = tblColRef.getTable() + "." + tblColRef.getName();
                if (columnName.toLowerCase().contains(limitedColumn)) {
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
        //cover "select * from table"
        Collection<TblColRef> requiredColumns = (realization instanceof HybridInstance) ? ((HybridInstance) realization).getAllColumns() : columns;
        columnFilter(requiredColumns, olapAuthentication);
        TupleFilter tupleFilter = rowFilter(realization, olapAuthentication);
        return tupleFilter;
    }
}
