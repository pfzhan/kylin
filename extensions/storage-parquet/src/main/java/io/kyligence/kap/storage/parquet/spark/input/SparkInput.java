package io.kyligence.kap.storage.parquet.spark.input;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.ParquetTarballFileInputFormat;
import io.kyligence.kap.storage.parquet.format.serialize.SerializableImmutableRoaringBitmap;
import scala.Tuple2;

public class SparkInput {
    public static final Logger logger = LoggerFactory.getLogger(SparkInput.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        assert (args.length >= 1);
        String path = args[0];
        logger.info(path);
        JavaSparkContext context = new JavaSparkContext(new SparkConf());
        Configuration config = new Configuration();

        // Create bitset
        ImmutableRoaringBitmap bitmap = createBitset(3);
        SerializableImmutableRoaringBitmap sBitmap = new SerializableImmutableRoaringBitmap(bitmap);

        // Set page bitmap
        HashMap<String, SerializableImmutableRoaringBitmap> pageMap = new HashMap<>();
        pageMap.put(path, sBitmap);
        logger.info("path put: " + path);
        //        config.set(ParquetFormatConstants.KYLIN_FILTER_PAGE_BITSET_MAP, serialize(pageMap));

        //        // Set measures column bitmap
        //        HashMap<String, SerializableImmutableRoaringBitmap> measureMap = new HashMap<>();
        //        measureMap.put(path, sBitmap);
        //        config.set(ParquetFormatConstants.KYLIN_FILTER_MEASURES_BITSET_MAP, serialize(measureMap));

        // Read parquet file and
        JavaPairRDD<byte[], byte[]> rdd = context.newAPIHadoopFile(path, ParquetTarballFileInputFormat.class, byte[].class, byte[].class, config);
        JavaRDD<Integer> rdd2 = rdd.map(new Function<Tuple2<byte[], byte[]>, Integer>() {
            @Override
            public Integer call(Tuple2<byte[], byte[]> tuple) throws Exception {
                //                System.out.println("Key: " + tuple._1().getBytes());
                //                System.out.println("Value: " + tuple._2().getBytes());
                return 0;
            }
        });
        rdd2.collect();
    }

    private static ImmutableRoaringBitmap createBitset(int total) throws IOException {
        MutableRoaringBitmap mBitmap = new MutableRoaringBitmap();
        for (int i = 0; i < total; ++i) {
            mBitmap.add(i);
        }

        ImmutableRoaringBitmap iBitmap = null;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos)) {
            mBitmap.serialize(dos);
            dos.flush();
            iBitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(baos.toByteArray()));
        }

        return iBitmap;
    }

    private static String serialize(HashMap<String, SerializableImmutableRoaringBitmap> map) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(map);
        oos.close();
        return new String(bos.toByteArray(), "ISO-8859-1");
    }
}
