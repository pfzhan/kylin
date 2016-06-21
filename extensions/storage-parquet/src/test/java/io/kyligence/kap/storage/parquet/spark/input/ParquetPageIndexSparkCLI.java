package io.kyligence.kap.storage.parquet.spark.input;

import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;
import io.kyligence.kap.storage.parquet.format.pageIndex.format.ParquetPageIndexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.UnitTestSupport;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by dong on 6/21/16.
 */
public class ParquetPageIndexSparkCLI {
    static GTInfo INFO;

    static File indexFile;
    final int dataSize = 50;
    final int cardinality = 50;

    int[] data;
    int[] columnLength = { Integer.SIZE - Integer.numberOfLeadingZeros(dataSize)};
    int[] cardinalities = { cardinality };
    String[] columnName = { "num" };
    boolean[] onlyEq = { false };

    private void writeIndexFile() throws IOException {
        data = new int[dataSize];
        for (int i = 0; i < dataSize; i++) {
            data[i] = i;
        }

        ParquetPageIndexWriter writer = new ParquetPageIndexWriter(columnName, columnLength, cardinalities, onlyEq, new DataOutputStream(new FileOutputStream(indexFile)));
        for (int i = 0; i < dataSize; i++) {
            byte[] buffer = new byte[columnLength[0]];
            BytesUtil.writeUnsigned(data[i], buffer, 0, columnLength[0]);
            writer.write(buffer, 0, i);
        }
        writer.close();
    }

    private GTScanRequest prepareRequest() throws IOException {
        indexFile = File.createTempFile("parquet", ".inv");

        INFO = UnitTestSupport.hllInfo();
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        filter.addChild(new ColumnTupleFilter(ColumnDesc.mockup(null, 1, "1", "int").getRef()));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data[0], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));

        GTScanRequest scanRequest = new GTScanRequest(INFO, null, new ImmutableBitSet(0, 3), new ImmutableBitSet(1, 3), new ImmutableBitSet(3, 6), new String[] { "SUM", "SUM", "COUNT_DISTINCT" }, filter, true, 0.5);
        return scanRequest;
    }

    public static void main(String args[]) {
        JavaSparkContext context = new JavaSparkContext(new SparkConf());
        Configuration config = new Configuration();
        GTScanRequest request = null;
        ByteBuffer buffer = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);
        GTScanRequest.serializer.serialize(request, buffer);
        String requestStr = new String(buffer.array());
        config.set(ParquetFormatConstants.KYLIN_SCAN_REQUEST, requestStr);

        JavaPairRDD<Text, Text> rdd = context.newAPIHadoopFile(indexFile.getAbsolutePath(), ParquetPageIndexInputFormat.class, Text.class, Text.class, config);
        JavaRDD<Integer> rdd2 = rdd.map(new Function<Tuple2<Text,Text>, Integer>() {
            @Override
            public Integer call(Tuple2<Text, Text> tuple) throws Exception {
                System.out.println("Key: " + tuple._1());
                System.out.println("Value: " + tuple._2());
                return 0;
            }
        });
        rdd2.collect();
    }
}
