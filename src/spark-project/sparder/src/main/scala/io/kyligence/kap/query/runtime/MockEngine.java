package io.kyligence.kap.query.runtime;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import io.kyligence.kap.query.MockContext;
import io.kyligence.kap.query.engine.exec.sparder.QueryEngine;

import java.util.List;

public class MockEngine implements QueryEngine {

    @Override
    public List<List<String>> compute(DataContext dataContext, RelNode relNode) {
        return null;
    }
}
