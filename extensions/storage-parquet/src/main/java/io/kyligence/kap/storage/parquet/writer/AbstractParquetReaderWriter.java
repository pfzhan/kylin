package io.kyligence.kap.storage.parquet.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractParquetReaderWriter {
    private final Map<String, CompressionCodec> codecByName = new HashMap<String, CompressionCodec>();

    protected CompressionCodec getCodec(CompressionCodecName codecName, Configuration config) {
        String codecClassName = codecName.getHadoopCompressionCodecClassName();
        if (codecClassName == null) {
            return null;
        }
        CompressionCodec codec = codecByName.get(codecClassName);
        if (codec != null) {
            return codec;
        }

        try {
            Class<?> codecClass = Class.forName(codecClassName);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, config);
            codecByName.put(codecClassName, codec);
            return codec;
        } catch (ClassNotFoundException e) {
            throw new BadConfigurationException("Class " + codecClassName + " was not found", e);
        }
    }
}
