package io.kyligence.kap.storage.parquet.spark.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;
import io.kyligence.kap.storage.parquet.format.pageIndex.format.ParquetPageIndexInputFormat;
import io.kyligence.kap.storage.parquet.format.serialize.SerializableImmutableRoaringBitmap;
import scala.Tuple2;

public class ParquetPageIndexSparkCLI {
    static GTInfo INFO;
    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexSparkCLI.class);

    final int dataSize = 50;
    final int cardinality = 50;

    int[] data;
    int[] columnLength = { Integer.SIZE - Integer.numberOfLeadingZeros(dataSize) };
    int[] cardinalities = { cardinality };
    String[] columnName = { "num" };
    boolean[] onlyEq = { false };

    public void writeIndexFile(Configuration conf) throws IOException {
        data = new int[dataSize];
        for (int i = 0; i < dataSize; i++) {
            data[i] = i;
        }
        for (int j = 0; j < 10; j++) {
            FSDataOutputStream outputStream = FileSystem.get(conf).create(new Path("/tmp/index/" + j + ".inv"));
            ParquetPageIndexWriter writer = new ParquetPageIndexWriter(columnName, columnLength, cardinalities, onlyEq, outputStream);
            for (int i = 0; i < dataSize; i++) {
                byte[] buffer = new byte[columnLength[0]];
                BytesUtil.writeUnsigned(data[i], buffer, 0, columnLength[0]);
                writer.write(buffer, 0, i);
            }
            writer.close();
        }
    }

    //    public GTScanRequest prepareRequest() throws IOException {
    //        INFO = UnitTestSupport.hllInfo();
    //        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
    //        filter.addChild(new ColumnTupleFilter(ColumnDesc.mockup(null, 1, "1", "int").getRef()));
    //        byte[] buffer = new byte[columnLength[0]];
    //        BytesUtil.writeUnsigned(data[0], buffer, 0, buffer.length);
    //        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
    //
    //        GTScanRequest scanRequest = new GTScanRequest(INFO, null, new ImmutableBitSet(0, 3), new ImmutableBitSet(1, 3), new ImmutableBitSet(3, 6), new String[] { "SUM", "SUM", "COUNT_DISTINCT" }, filter, true, 0.5);
    //        return scanRequest;
    //    }

    public static void main(String args[]) throws IOException, ClassNotFoundException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class<?>[]{
                Class.forName("io.kyligence.kap.storage.parquet.format.serialize.SerializableImmutableRoaringBitmap"),
                Class.forName("org.apache.hadoop.io.Text")
        });
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        Configuration config = new Configuration();
        ParquetPageIndexSparkCLI app = new ParquetPageIndexSparkCLI();
        app.writeIndexFile(config);
        //        GTScanRequest request = app.prepareRequest();
        //        ByteBuffer buffer = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);
        //        GTScanRequest.serializer.serialize(request, buffer);
        //        String requestStr = new String(buffer.array());
        //        config.set(ParquetFormatConstants.KYLIN_SCAN_REQUEST, requestStr);

        JavaPairRDD<Text, SerializableImmutableRoaringBitmap> rdd = context.newAPIHadoopFile("/tmp/index/*.inv", ParquetPageIndexInputFormat.class, Text.class, SerializableImmutableRoaringBitmap.class, config);
        JavaRDD<Tuple2<String, SerializableImmutableRoaringBitmap>> rdd2 = rdd.map(new Function<Tuple2<Text, SerializableImmutableRoaringBitmap>, Tuple2<String, SerializableImmutableRoaringBitmap>>() {
            @Override
            public Tuple2<String, SerializableImmutableRoaringBitmap> call(Tuple2<Text, SerializableImmutableRoaringBitmap> tuple) throws Exception {
                logger.info("Key: " + tuple._1().toString());
                logger.info("Value: " + tuple._2().getBitmap().toString());
                return new Tuple2<>(tuple._1.toString(), tuple._2);
            }
        });
        List<Tuple2<String, SerializableImmutableRoaringBitmap>> result = rdd2.collect();
        for (Tuple2<String, SerializableImmutableRoaringBitmap> tuple : result) {
            logger.info("result: key: " + tuple._1);
            logger.info("result: value: " + tuple._2.getBitmap());
        }
    }
}
