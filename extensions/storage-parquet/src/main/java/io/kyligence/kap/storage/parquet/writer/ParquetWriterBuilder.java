package io.kyligence.kap.storage.parquet.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.Encoding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetWriterBuilder {
    private String indexPathSuffix = "index";
    private Configuration conf = null;
    private MessageType type = null;
    private Path path = null;
    private Encoding rlEncodings = Encoding.RLE;
    private Encoding dlEncodings = Encoding.RLE;
    private List<Encoding> dataEncodings = null;
    private CompressionCodecName codecName = CompressionCodecName.UNCOMPRESSED;
    private Path indexPath = null;

    public ParquetWriterBuilder setIndexPathSuffix(String indexPathSuffix) {
        this.indexPathSuffix = indexPathSuffix;
        return this;
    }

    public ParquetWriterBuilder setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }


    public ParquetWriterBuilder setType(MessageType type) {
        this.type = type;
        return this;
    }

    public ParquetWriterBuilder setPath(Path path) {
        this.path = path;
        return this;
    }

    public ParquetWriterBuilder setRlEncodings(Encoding rlEncodings) {
        this.rlEncodings = rlEncodings;
        return this;
    }

    public ParquetWriterBuilder setDlEncodings(Encoding dlEncodings) {
        this.dlEncodings = dlEncodings;
        return this;
    }

    public ParquetWriterBuilder setDataEncodings(List<Encoding> dataEncodings) {
        this.dataEncodings = dataEncodings;
        return this;
    }

    public ParquetWriterBuilder setCodecName(CompressionCodecName codecName) {
        this.codecName = codecName;
        return this;
    }

    public ParquetWriterBuilder setIndexPath(Path indexPath) {
        this.indexPath = indexPath;
        return this;
    }

    public ParquetWriterBuilder(){}

    public ParquetWriter build() throws IOException {
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
                switch(type.getColumns().get(i).getType()) {
                    case BOOLEAN:
                    case INT32:
                        dataEncodings.add(Encoding.RLE);
                        break;
                    case FIXED_LEN_BYTE_ARRAY:
                        dataEncodings.add(Encoding.DELTA_BYTE_ARRAY);
                    default:
                        dataEncodings.add(Encoding.PLAIN);
                }
            }
        }

        if (indexPath == null) {
            indexPath = new Path(path.toString() + indexPathSuffix);
        }

        return new ParquetWriter(conf, type, path, rlEncodings, dlEncodings, dataEncodings, codecName, indexPath);
    }
}
