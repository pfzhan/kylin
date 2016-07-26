package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetRawWriterBuilder {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetRawWriterBuilder.class);

    private String indexPathSuffix = "index";
    private Configuration conf = null;
    private MessageType type = null;
    private Path path = null;
    private Encoding rlEncodings = Encoding.RLE;
    private Encoding dlEncodings = Encoding.RLE;
    private List<Encoding> dataEncodings = null;
    private CompressionCodecName codecName = CompressionCodecName.UNCOMPRESSED;
    private Path indexPath = null;
    private int rowsPerPage = 10000;
    private int pagesPerGroup = 100;

    public ParquetRawWriterBuilder setIndexPathSuffix(String indexPathSuffix) {
        this.indexPathSuffix = indexPathSuffix;
        return this;
    }

    public ParquetRawWriterBuilder setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }

    public ParquetRawWriterBuilder setType(MessageType type) {
        this.type = type;
        return this;
    }

    public ParquetRawWriterBuilder setPath(Path path) {
        this.path = path;
        return this;
    }

    public ParquetRawWriterBuilder setRlEncodings(Encoding rlEncodings) {
        this.rlEncodings = rlEncodings;
        return this;
    }

    public ParquetRawWriterBuilder setDlEncodings(Encoding dlEncodings) {
        this.dlEncodings = dlEncodings;
        return this;
    }

    public ParquetRawWriterBuilder setDataEncodings(List<Encoding> dataEncodings) {
        this.dataEncodings = dataEncodings;
        return this;
    }

    public ParquetRawWriterBuilder setCodecName(String codecName) {
        if (StringUtils.isEmpty(codecName)) {
            this.codecName = CompressionCodecName.UNCOMPRESSED;
        }

        CompressionCodecName compressionCodecName;
        try {
            compressionCodecName = CompressionCodecName.valueOf(codecName.toUpperCase());
        } catch (Exception e) {
            compressionCodecName = CompressionCodecName.UNCOMPRESSED;
        }

        logger.info("The chosen CompressionCodecName is " + this.codecName);
        this.codecName = compressionCodecName;
        return this;
    }

    public ParquetRawWriterBuilder setIndexPath(Path indexPath) {
        this.indexPath = indexPath;
        return this;
    }

    public ParquetRawWriterBuilder setRowsPerPage(int rowsPerPage) {
        this.rowsPerPage = rowsPerPage;
        return this;
    }

    public ParquetRawWriterBuilder setPagesPerGroup(int pagesPerGroup) {
        this.pagesPerGroup = pagesPerGroup;
        return this;
    }

    public ParquetRawWriterBuilder() {
    }

    public ParquetRawWriter build() throws IOException {
        if (conf == null) {
            throw new IllegalStateException("Configuration should be set");
        }
        if (type == null) {
            throw new IllegalStateException("Schema should be set");
        }
        if (path == null) {
            throw new IllegalStateException("Output file path should be set");
        }

        if (dataEncodings == null) {
            dataEncodings = new ArrayList<Encoding>();
            for (int i = 0; i < type.getColumns().size(); ++i) {
                switch (type.getColumns().get(i).getType()) {
                case BOOLEAN:
                case INT32:
                    dataEncodings.add(Encoding.RLE);
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                case BINARY:
                    dataEncodings.add(Encoding.PLAIN);
                    break;
                default:
                    dataEncodings.add(Encoding.PLAIN);
                    break;
                }
            }
        }

        if (indexPath == null) {
            indexPath = new Path(path.toString() + indexPathSuffix);
        }

        logger.info("ParquetRawWriterBuilder: rowsPerPage={}", rowsPerPage);
        return new ParquetRawWriter(conf, type, path, rlEncodings, dlEncodings, dataEncodings, codecName, indexPath, rowsPerPage, pagesPerGroup);
    }
}
