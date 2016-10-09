package io.kyligence.kap.query.security;

import java.util.Collection;
import java.util.Map;

import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPAuthentication;

public interface IRowFilter {
    /*
    * Extend the row filter by given conditions
    * @authentication user authentication
    * @allCubeColumns all columns in current cube
    * @conditions filter condition
    *
    * Basically, its implementation will be finished by Cube Builder who should understand the whole cube and then
    * set the row filter conditions.
    */
    public TupleFilter getRowFilter(OLAPAuthentication authentication, Collection<TblColRef> allCubeColumns, Map<String, String> conditions);
}
