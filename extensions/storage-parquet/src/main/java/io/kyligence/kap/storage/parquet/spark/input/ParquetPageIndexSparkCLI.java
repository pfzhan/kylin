/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

//package io.kyligence.kap.storage.parquet.spark.input;
//
//import java.io.IOException;
//import java.util.List;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.kylin.common.util.ByteArray;
//import org.apache.kylin.common.util.BytesUtil;
//import org.apache.kylin.metadata.filter.ColumnTupleFilter;
//import org.apache.kylin.metadata.filter.CompareTupleFilter;
//import org.apache.kylin.metadata.filter.ConstantTupleFilter;
//import org.apache.kylin.metadata.filter.TupleFilter;
//import org.apache.kylin.metadata.model.ColumnDesc;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexTable;
//import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;
//import io.kyligence.kap.storage.parquet.format.serialize.SerializableImmutableRoaringBitmap;
//import scala.Tuple2;
//
//public class ParquetPageIndexSparkCLI {
//    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexSparkCLI.class);
//
//    final int dataSize = 50;
//    final int cardinality = 50;
//
//    int[] data;
//    int[] columnLength = { Integer.SIZE - Integer.numberOfLeadingZeros(dataSize) };
//    int[] cardinalities = { cardinality };
//    String[] columnName = { "num" };
//    boolean[] onlyEq = { false };
//
//    public void writeIndexFile(Configuration conf) throws IOException {
//        data = new int[dataSize];
//        for (int i = 0; i < dataSize; i++) {
//            data[i] = i;
//        }
//        for (int j = 0; j < 10; j++) {
//            FSDataOutputStream outputStream = FileSystem.get(conf).create(new Path("/tmp/index/" + j + ".inv"));
//            ParquetPageIndexWriter writer = new ParquetPageIndexWriter(columnName, columnLength, cardinalities, onlyEq, outputStream);
//            for (int i = 0; i < dataSize; i++) {
//                byte[] buffer = new byte[columnLength[0]];
//                BytesUtil.writeUnsigned(data[i], buffer, 0, columnLength[0]);
//                writer.write(buffer, 0, i);
//            }
//            writer.close();
//        }
//    }
//
//    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        SparkConf sparkConf = new SparkConf();
//        //        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        //        sparkConf.registerKryoClasses(new Class<?>[]{
//        //                Class.forName("io.kyligence.kap.storage.parquet.format.serialize.SerializableImmutableRoaringBitmap"),
//        //                Class.forName("org.apache.hadoop.io.Text")
//        //        });
//        JavaSparkContext context = new JavaSparkContext(sparkConf);
//
//        // build index for test
//        Configuration config = new Configuration();
//        ParquetPageIndexSparkCLI app = new ParquetPageIndexSparkCLI();
//        app.writeIndexFile(config);
//
//        JavaPairRDD<String, ParquetPageIndexTable> rdd = context.newAPIHadoopFile("/tmp/index/*.inv", ParquetPageIndexInputFormat.class, String.class, ParquetPageIndexTable.class, config);
//        JavaRDD<Tuple2<String, SerializableImmutableRoaringBitmap>> rdd2 = rdd.map(new Function<Tuple2<String, ParquetPageIndexTable>, Tuple2<String, SerializableImmutableRoaringBitmap>>() {
//            @Override
//            public Tuple2<String, SerializableImmutableRoaringBitmap> call(Tuple2<String, ParquetPageIndexTable> tuple) throws Exception {
//                // build test filter, will get from broadcast in real case
//                int dataSize = 50;
//                int columnLength = Integer.SIZE - Integer.numberOfLeadingZeros(dataSize);
//                TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
//                filter.addChild(new ColumnTupleFilter(ColumnDesc.mockup(null, 1, "1", null).getRef()));
//                byte[] buffer = new byte[columnLength];
//                BytesUtil.writeUnsigned(10, buffer, 0, buffer.length);
//                filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
//
//                // lookup index table
//                SerializableImmutableRoaringBitmap result = new SerializableImmutableRoaringBitmap(tuple._2().lookup(filter));
//
//                logger.info("InMap: Key: " + tuple._1());
//                logger.info("InMap: Value: " + result);
//
//                return new Tuple2<>(tuple._1(), result);
//            }
//        });
//
//        // print results
//        List<Tuple2<String, SerializableImmutableRoaringBitmap>> result = rdd2.collect();
//        for (Tuple2<String, SerializableImmutableRoaringBitmap> tuple : result) {
//            logger.info("result: key: " + tuple._1);
//            logger.info("result: value: " + tuple._2.getBitmap());
//        }
//    }
//}
