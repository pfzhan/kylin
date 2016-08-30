package io.kyligence.kap.tool.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.dimension.IntegerDimEnc;
import org.apache.kylin.metadata.datatype.BigDecimalSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.datatype.DateTimeSerializer;
import org.apache.kylin.metadata.datatype.LongSerializer;
import org.apache.kylin.metadata.datatype.StringSerializer;
import org.apache.parquet.io.api.Binary;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReaderBuilder;

/**
 * This tool is used to print raw table parquet file
 */
public class RawTableParquetFilePrinter {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            return;
        }
        String path = args[0];
        PrintFile(path);
    }

    public static void PrintFile(String path) throws IOException {

        DateTimeSerializer dateSe = new DateTimeSerializer(null);
        StringSerializer stringSe = new StringSerializer(new DataType("A", 1, 1));
        LongSerializer longSe = new LongSerializer(null);
        IntegerDimEnc.IntegerSerializer intSe = (IntegerDimEnc.IntegerSerializer) new IntegerDimEnc().asDataTypeSerializer();
        BigDecimalSerializer bigdecSe = new BigDecimalSerializer(new DataType("B", 1, 1));

        //DataTypeSerializer[] ses = new DataTypeSerializer[] { dateSe, dateSe, dateSe, stringSe, stringSe, stringSe, stringSe, stringSe, stringSe, stringSe, stringSe, longSe, bigdecSe, longSe, longSe, intSe, longSe, intSe };
        DataTypeSerializer[] ses = new DataTypeSerializer[] { dateSe, intSe, intSe, stringSe, intSe, intSe, stringSe, intSe, bigdecSe, intSe };
        ParquetBundleReader bundleReader = new ParquetBundleReaderBuilder().setPath(new Path(path)).setConf(new Configuration()).build();
        int count = 0;
        while (true) {
            List<Object> data = bundleReader.read();
            if (data == null) {
                break;
            }

            for (int i = 0; i < data.size(); i++) {
                Binary d = (Binary) data.get(i);
                if (i < ses.length && ses[i] != null) {
                    des(d, ses[i]);
                } else {
                    System.out.print(new String(d.getBytes()) + "\t");
                }
            }
            count++;
            System.out.println();
        }
        System.out.println(count);
    }

    private static void des(Binary binary, DataTypeSerializer<?> se) {
        byte[] b = binary.getBytes();
        System.out.print(se.deserialize(ByteBuffer.wrap(b)) + ",\t");
    }

}
