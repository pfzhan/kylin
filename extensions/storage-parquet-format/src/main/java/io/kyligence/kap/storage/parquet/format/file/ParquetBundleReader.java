package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetBundleReader {
    public static final Logger logger = LoggerFactory.getLogger(ParquetBundleReader.class);

    List<ParquetReaderState> readerStates;

    public ParquetBundleReader(Configuration configuration, Path path, Path indexPath, ImmutableRoaringBitmap columns, ImmutableRoaringBitmap pageBitset, long fileOffset) throws IOException {
        readerStates = new ArrayList<ParquetReaderState>(columns.getCardinality());

        for (int column : columns) {
            readerStates.add(new ParquetReaderState(new ParquetColumnReaderBuilder().setFileOffset(fileOffset).setConf(configuration).setPath(path).setIndexPath(indexPath).setColumn(column).setPageBitset(pageBitset).build()));
            logger.info("Read Column: " + column);
        }
    }

    public List<Object> read() throws IOException {
        List<Object> result = new ArrayList<Object>();
        for (ParquetReaderState state : readerStates) {
            GeneralValuesReader valuesReader = state.getValuesReader();
            
            if (valuesReader == null) {
                return null;
            }
            
            Object value = valuesReader.readData();

            if (value == null) {
                if ((valuesReader = getNextValuesReader(state)) == null) {
                    return null;
                }
                value = valuesReader.readData();
                if (value == null) {
                    return null;
                }
            }

            result.add(value);
        }
        return result;
    }

    public void close() throws IOException {
        for (ParquetReaderState state : readerStates) {
            state.reader.close();
        }
    }

    private GeneralValuesReader getNextValuesReader(ParquetReaderState state) throws IOException {
        GeneralValuesReader valuesReader = state.getNextValuesReader();
        state.setValuesReader(valuesReader);
        return valuesReader;
    }

    private class ParquetReaderState {
        private ParquetColumnReader reader;
        private GeneralValuesReader valuesReader;

        public ParquetReaderState(ParquetColumnReader reader) throws IOException {
            this.reader = reader;
            this.valuesReader = reader.getNextValuesReader();
        }

        public GeneralValuesReader getNextValuesReader() throws IOException {
            return reader.getNextValuesReader();
        }

        public GeneralValuesReader getValuesReader() {
            return valuesReader;
        }

        public void setReader(ParquetColumnReader reader) {
            this.reader = reader;
        }

        public void setValuesReader(GeneralValuesReader valuesReader) {
            this.valuesReader = valuesReader;
        }
    }
}
