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
package io.kyligence.kap.engine.spark.dict;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.common.DebugFilesystem;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.builder.NBucketDictionary;
import io.kyligence.kap.engine.spark.builder.NGlobalDictHDFSStore;
import io.kyligence.kap.engine.spark.builder.NGlobalDictMetadata;
import io.kyligence.kap.engine.spark.builder.NGlobalDictStore;
import io.kyligence.kap.engine.spark.builder.NGlobalDictionaryV2;
import io.kyligence.kap.engine.spark.builder.NHashPartitioner;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import scala.Tuple2;

public class NGlobalDictionaryV2Test extends NLocalWithSparkSessionTest {

    private final static int BUCKET_SIZE = 10;

    private final static int RANDOM_DATA_SIZE = 1000;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        initFs();
    }

    @Override
    public void tearDown() throws Exception {
        DebugFilesystem.assertNoOpenStreams();
        super.tearDown();
    }

    @Test
    public void testGlobalDictionaryRoundTest() throws IOException {

        // round 1
        roundTest();

        // round 2
        roundTest();

        // round 3
        roundTest();
    }

    private void roundTest() throws IOException {
        System.out.println("NGlobalDictionaryV2Test -> roundTest -> " + System.currentTimeMillis());
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NGlobalDictionaryV2 dict1 = new NGlobalDictionaryV2("a", "spark", config.getHdfsWorkingDirectory());
        NGlobalDictionaryV2 dict2 = new NGlobalDictionaryV2("a", "local", config.getHdfsWorkingDirectory());
        List<String> stringList = generateRandomData();
        Collections.sort(stringList);
        runWithSparkBuildGlobalDict(dict1, stringList);
        runWithLocalBuildGlobalDict(dict2, stringList);
        compareTwoVersionDict(dict1, dict2);
        compareTwoModeVersionNum(dict1, dict2);
    }

    private List<String> generateRandomData() {
        List<String> stringList = Lists.newArrayList();
        for (int i = 0; i < RANDOM_DATA_SIZE; i++) {
            stringList.add(RandomStringUtils.randomAlphabetic(10));
        }
        return stringList;
    }

    private void runWithSparkBuildGlobalDict(NGlobalDictionaryV2 dict, List<String> stringSet) throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        dict.prepareWrite();
        List<Row> rowList = Lists.newLinkedList();
        for (String str : stringSet) {
            rowList.add(RowFactory.create(str));
        }
        Dataset<Row> ds = ss.createDataFrame(rowList,
                new StructType(new StructField[] { DataTypes.createStructField("col1", DataTypes.StringType, true) }));
        ds.toJavaRDD().mapToPair((PairFunction<Row, String, String>) row -> {
            if (row.get(0) == null)
                return new Tuple2<>(null, null);
            return new Tuple2<>(row.get(0).toString(), null);
        }).sortByKey().partitionBy(new NHashPartitioner(BUCKET_SIZE)).mapPartitionsWithIndex(
                (Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<Object>>) (bucketId, tuple2Iterator) -> {
                    NBucketDictionary bucketDict = dict.createBucketDictionary(bucketId);
                    while (tuple2Iterator.hasNext()) {
                        Tuple2<String, String> tuple2 = tuple2Iterator.next();
                        bucketDict.addValue(tuple2._1);
                    }
                    bucketDict.saveBucketDict(bucketId);
                    return Lists.newArrayList().iterator();
                }, true).count();

        dict.writeMetaDict(config.getGlobalDictV2MaxVersions(), config.getGlobalDictV2VersionTTL());
    }

    private void runWithLocalBuildGlobalDict(NGlobalDictionaryV2 dict, List<String> stringSet) throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        dict.prepareWrite();
        NHashPartitioner partitioner = new NHashPartitioner(BUCKET_SIZE);
        Map<Integer, List<String>> vmap = new HashMap<>();
        for (String string : stringSet) {
            int bucketId = partitioner.getPartition(string);
            if (vmap.containsKey(bucketId)) {
                vmap.get(bucketId).add(string);
            } else {
                List<String> list = Lists.newArrayList();
                list.add(string);
                vmap.put(bucketId, list);
            }
        }

        for (Map.Entry<Integer, List<String>> entry : vmap.entrySet()) {
            NBucketDictionary bucketDict = dict.createBucketDictionary(entry.getKey());
            for (String s : entry.getValue()) {
                bucketDict.addValue(s);
            }
            bucketDict.saveBucketDict(entry.getKey());
        }

        dict.writeMetaDict(config.getGlobalDictV2MaxVersions(), config.getGlobalDictV2VersionTTL());
    }

    private void compareTwoModeVersionNum(NGlobalDictionaryV2 dict1, NGlobalDictionaryV2 dict2) throws IOException {
        NGlobalDictStore store1 = new NGlobalDictHDFSStore(dict1.getResourceDir());
        NGlobalDictStore store2 = new NGlobalDictHDFSStore(dict2.getResourceDir());
        Assert.assertEquals(store1.listAllVersions().length, store2.listAllVersions().length);
    }

    private void compareTwoVersionDict(NGlobalDictionaryV2 dict1, NGlobalDictionaryV2 dict2) throws IOException {
        NGlobalDictMetadata metadata1 = dict1.getMetaDict();
        NGlobalDictMetadata metadata2 = dict2.getMetaDict();

        // compare dict meta info
        Assert.assertEquals(metadata1.getDictCount(), metadata2.getDictCount());
        Assert.assertEquals(metadata1.getBucketSize(), metadata2.getBucketSize());
        Assert.assertArrayEquals(metadata1.getOffset(), metadata2.getOffset());

        for (int i = 0; i < metadata1.getBucketSize(); i++) {
            NBucketDictionary bucket1 = dict1.createBucketDictionary(i);
            NBucketDictionary bucket2 = dict2.createBucketDictionary(i);

            Object2IntMap<String> map1 = bucket1.getPrevObject2IntMap();
            Object2IntMap<String> map2 = bucket2.getPrevObject2IntMap();
            for (Object2IntMap.Entry<String> entry : map1.object2IntEntrySet()) {
                Assert.assertEquals(entry.getIntValue(), map2.getInt(entry.getKey()));
            }
        }
    }

    private void initFs() {
        DebugFilesystem.clearOpenStreams();
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", DebugFilesystem.class.getCanonicalName());
        conf.set("fs.file.impl.disable.cache", "true");
        HadoopUtil.setCurrentConfiguration(conf);
    }
}
