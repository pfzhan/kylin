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

package io.kyligence.kap.storage.parquet.spark.input;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
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
        JavaPairRDD<Text, Text> rdd = context.newAPIHadoopFile(path, ParquetTarballFileInputFormat.class, Text.class, Text.class, config);
        JavaRDD<Integer> rdd2 = rdd.map(new Function<Tuple2<Text, Text>, Integer>() {
            @Override
            public Integer call(Tuple2<Text, Text> tuple) throws Exception {
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
