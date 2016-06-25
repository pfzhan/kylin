package io.kyligence.kap.storage.parquet.format.file;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class CodecFactory {
    private static ConcurrentMap<String, CompressionCodec> codecByName = new ConcurrentHashMap<>();

    public static CompressionCodec getCodec(CompressionCodecName codecName, Configuration config) {
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
