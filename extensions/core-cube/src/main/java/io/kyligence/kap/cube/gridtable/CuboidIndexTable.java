package io.kyligence.kap.cube.gridtable;

import org.apache.kylin.metadata.filter.CompareTupleFilter;

import io.kyligence.kap.cube.index.IIndexTable;

/**
 * for query on cuboid 11111 we might be able to use 11110 as the index table
 */
public class CuboidIndexTable implements IIndexTable {

    @Override
    public GTScanRanges lookup(CompareTupleFilter filter) {
        return null;
    }
}
