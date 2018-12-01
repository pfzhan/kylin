package io.kyligence.kap.query.runtime;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import io.kyligence.kap.query.MockContext;
import io.kyligence.kap.query.exec.QueryEngine;

public class MockEngine implements QueryEngine {
    @Override
    public Enumerable<Object> computeSCALA(DataContext dataContext, RelNode relNode, RelDataType resultType) {
        MockContext.current().setDataContext(dataContext);
        MockContext.current().setRelDataType(resultType);
        MockContext.current().setRelNode(relNode);

        return Linq4j.emptyEnumerable();
    }

    @Override
    public Enumerable<Object[]> compute(DataContext dataContext, RelNode relNode, RelDataType resultType) {
        MockContext.current().setDataContext(dataContext);
        MockContext.current().setRelDataType(resultType);
        MockContext.current().setRelNode(relNode);
        return Linq4j.emptyEnumerable();
    }
}
