package io.kyligence.kap.query.security;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPAuthentication;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KapAccessDecisionMaker implements OLAPContext.IAccessController {
    private final static Logger logger = LoggerFactory.getLogger(KapAccessDecisionMaker.class);
    private final static KapAclReader aclFile = new KapAclReader();
    private Hashtable<TblColRef, String> cubeAccessControlColumns = new Hashtable<>();

    @Override
    public TupleFilter check(OLAPAuthentication olapAuthentication, Collection<TblColRef> columns, IRealization realization) throws IllegalArgumentException {

        boolean isACLEnable = KapConfig.getInstanceFromEnv().getCellLevelSecurityEnable().equalsIgnoreCase("true");

        if (isACLEnable) {
            logger.info("User access control is enable!!!!");
        } else {
            return null;
        }
        if (null == olapAuthentication.getUsername())
            return null;

        aclFile.loadAndValidateACL(realization.getDataModelDesc());

        if (!aclFile.isAvailable(olapAuthentication.getUsername()))
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

    private TupleFilter rowFilter(IRealization realization, OLAPAuthentication olapAuthentication) {

        if (null == realization || !(realization instanceof CubeInstance))
            return null;
        CubeDesc cubeDesc = ((CubeInstance) realization).getDescriptor();
        String currentUser = olapAuthentication.getUsername().toLowerCase();
        Hashtable<String, String> aclAttributes = aclFile.accessControlColumnsByUser.get(currentUser);
        return getRowFilter(cubeDesc.listAllColumns(), aclAttributes);
    }

    private void columnFilter(Collection<TblColRef> columns, OLAPAuthentication olapAuthentication) {
        String currentUser = olapAuthentication.getUsername().toLowerCase();
        Hashtable<String, String> aclColumns = aclFile.accessControlColumnsByUser.get(currentUser);
        for (Map.Entry<String, String> attr : aclColumns.entrySet()) {
            for (TblColRef tblColRef : columns) {
                String colNamewithTable = tblColRef.getCanonicalName().toLowerCase();
                if (colNamewithTable.contains(attr.getKey()) && attr.getValue().equalsIgnoreCase(KapAclReader.DENY)) {
                    throw new IllegalArgumentException("Current User: " + currentUser + " is not allowed to access column: " + colNamewithTable);
                }
            }
        }
    }

    public TupleFilter getRowFilter(Collection<TblColRef> allCubeColumns, Hashtable<String, String> aclColumns) {
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
        return (value.equalsIgnoreCase(KapAclReader.DENY) || value.equalsIgnoreCase(KapAclReader.YES));
    }
}
