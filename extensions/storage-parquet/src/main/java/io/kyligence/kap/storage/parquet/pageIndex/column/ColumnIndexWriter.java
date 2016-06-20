package io.kyligence.kap.storage.parquet.pageIndex.column;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kylin.common.util.ByteArray;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Created by dong on 16/6/18.
 */
public class ColumnIndexWriter implements IColumnInvertedIndex.Builder<ByteArray> {
    protected static final Logger logger = LoggerFactory.getLogger(ColumnIndexWriter.class);

    private DataOutputStream outputStream;
    private NavigableMap<ByteArray, MutableRoaringBitmap> indexMap = Maps.newTreeMap();
    private ColumnSpec columnSpec;
    private int step;
    private long totalSize = -1;

    public ColumnIndexWriter(ColumnSpec columnSpec, DataOutputStream outputStream) {
        this.outputStream = outputStream;
        this.columnSpec = columnSpec;
        this.step = decideStepSize(columnSpec.getCardinality());
    }

    public long getTotalSize() {
        if (totalSize < 0) {
            throw new RuntimeException("cannot get totalSize before seal.");
        }
        return totalSize;
    }

    private int decideStepSize(int columnCardinality) {
        if (columnCardinality == 0) {
            return 1;
        } else {
            return 1;
        }
    }

    private void writeIndex(Map<ByteArray, MutableRoaringBitmap> indexRaw) throws IOException {
        // optimize compression
        for (MutableRoaringBitmap bitmap : indexRaw.values()) {
            bitmap.runOptimize();
        }

        // write index header
        int counter = 0;
        long position = 0;
        int headerSize = 0;
        for (Map.Entry<ByteArray, MutableRoaringBitmap> indexEntry : indexRaw.entrySet()) {
            ByteArray key = indexEntry.getKey();
            MutableRoaringBitmap value = indexEntry.getValue();

            if (counter++ % step == 0) {
                outputStream.write(key.array(), key.offset(), key.length());
                outputStream.writeLong(position);
                headerSize += key.length() + 8;
            }
            position += key.length() + value.serializedSizeInBytes();
        }

        // write body length of bytes
        outputStream.writeLong(position);
        headerSize += 8;

        // write body bytes
        for (Map.Entry<ByteArray, MutableRoaringBitmap> indexEntry : indexRaw.entrySet()) {
            ByteArray key = indexEntry.getKey();
            outputStream.write(key.array(), key.offset(), key.length());
            indexEntry.getValue().serialize(outputStream);
        }
        totalSize += headerSize + position;
        logger.info("Index Length Stats: Header={}, Body={}, Step={}", headerSize, position, step);
    }

    private void seal() throws IOException {
        // write metadata
        // TODO: Write ID of this index, such as magic number
        totalSize = 4 * 4;

        outputStream.writeInt(columnSpec.isOnlyEQIndex() ? 1 : 0);
        outputStream.writeInt(indexMap.size());
        outputStream.writeInt(step);
        outputStream.writeInt(columnSpec.getColumnLength());

        logger.info("onlyEQ={}, cardinality={}, columnLength={}, step={}", columnSpec.isOnlyEQIndex(), indexMap.size(), columnSpec.getColumnLength(), step);
        logger.info("Start to write eq index for column {}", columnSpec.getColumnName());
        writeIndex(indexMap);
        if (!columnSpec.isOnlyEQIndex()) {
            writeAuxiliary();
        }
    }

    private void writeAuxiliary() throws IOException {
        // write lt
        logger.info("Start to write lt index for column {}", columnSpec.getColumnName());

        NavigableMap<ByteArray, MutableRoaringBitmap> auxiliaryIndexMap = Maps.newTreeMap();
        MutableRoaringBitmap lastValue = MutableRoaringBitmap.bitmapOf();
        for (Map.Entry<ByteArray, MutableRoaringBitmap> indexEntry : indexMap.entrySet()) {
            auxiliaryIndexMap.put(indexEntry.getKey(), MutableRoaringBitmap.or(lastValue, indexEntry.getValue()));
        }
        writeIndex(auxiliaryIndexMap);

        // write gt
        logger.info("Start to write gt index for column {}", columnSpec.getColumnName());
        auxiliaryIndexMap = Maps.newTreeMap();
        lastValue = MutableRoaringBitmap.bitmapOf();
        for (Map.Entry<ByteArray, MutableRoaringBitmap> indexEntry : indexMap.descendingMap().entrySet()) {
            auxiliaryIndexMap.put(indexEntry.getKey(), MutableRoaringBitmap.or(lastValue, indexEntry.getValue()));
        }
        writeIndex(auxiliaryIndexMap);
    }

    @Override
    public void close() throws IOException {
        seal();
    }

    @Override
    public void putNextRow(ByteArray value) {
        throw new NotImplementedException();
    }

    @Override
    public void putNextRow(ByteArray[] value) {
        throw new NotImplementedException();
    }

    @Override
    public void appendToRow(ByteArray value, int row) {
        Preconditions.checkState(columnSpec.getColumnLength() == value.length());

        if (!indexMap.containsKey(value)) {
            indexMap.put(value, MutableRoaringBitmap.bitmapOf(row));
        } else {
            indexMap.get(value).add(row);
        }
    }

    @Override
    public void appendToRow(ByteArray[] values, int row) {
        if (values != null && values.length > 0) {
            for (ByteArray value : values) {
                appendToRow(value, row);
            }
        }
    }
}
