package io.kyligence.kap.cube.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.DateStrDictionary;
import org.apache.kylin.dict.TrieDictionary;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dongli on 6/2/16.
 */
public class ColumnInvertedIndexWriter implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ColumnInvertedIndexWriter.class);

    private int colOffset = 0;
    private int colLength;
    private IColumnInvertedIndex.Builder invertedIndexBuilder;

    /**
     * Only for UT
     *
     * @param colName
     * @param cardinality
     * @param colLength
     * @param ivdIdx
     */
    public ColumnInvertedIndexWriter(String colName, int cardinality, int colLength, File ivdIdx) {
        this.colLength = colLength;
        invertedIndexBuilder = ColumnIndexFactory.createLocalInvertedIndex(colName, cardinality, ivdIdx.getAbsolutePath()).rebuild();
    }

    /**
     * Constructor for real usage. Will check the dictionary type and use a proper cardinality
     *
     * @param col
     * @param dictionary
     * @param length
     * @param ivdIdx
     */
    public ColumnInvertedIndexWriter(TblColRef col, Dictionary<String> dictionary, int length, File ivdIdx) {
        this.colLength = length;
        int cardinality = dictionary.getSize();
        if (dictionary instanceof DateStrDictionary) {
            cardinality = cardinality / 4; // 0000 to 2500 year
        } else if (!(dictionary instanceof TrieDictionary)) {
            throw new IllegalArgumentException("Not support to build secondary dictionary for col " + col);
        }

        logger.info("Build inverted index for col " + col + ", cardinality is " + cardinality);
        if (cardinality > 1000000) {
            logger.warn("Ultra high cardinality column, may eat much memory.");
        }

        invertedIndexBuilder = ColumnIndexFactory.createLocalInvertedIndex(col.getName(), cardinality, ivdIdx.getAbsolutePath()).rebuild();
    }

    public void write(byte[] bytes, int rowId) throws IOException {
        write(bytes, 0, bytes.length, rowId);
    }

    public void write(byte[] bytes, int offset, int length, int rowId) throws IOException {
        assert length >= colLength && colOffset + offset + colLength <= bytes.length;

        int value = BytesUtil.readUnsigned(bytes, colOffset + offset, colLength);
        //write the value to the index files
        invertedIndexBuilder.appendToRow(value, rowId);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(invertedIndexBuilder);
    }
}
