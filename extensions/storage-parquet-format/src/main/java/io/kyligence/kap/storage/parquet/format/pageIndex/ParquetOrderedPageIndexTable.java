package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;

public class ParquetOrderedPageIndexTable extends ParquetPageIndexTable {

    private Set<Integer> orderedColumns = Sets.newHashSet();

    public ParquetOrderedPageIndexTable(FileSystem fileSystem, Path parquetIndexPath, FSDataInputStream inputStream, int startOffset, Collection<Integer> orderedColumns) throws IOException {
        super(fileSystem, parquetIndexPath, inputStream, startOffset);
        this.orderedColumns.addAll(orderedColumns);
    }

    protected boolean isColumnOrdered(int col) {
        return this.orderedColumns.contains(col);
    }
}
