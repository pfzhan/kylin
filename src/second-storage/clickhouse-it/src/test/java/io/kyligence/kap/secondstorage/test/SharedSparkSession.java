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
package io.kyligence.kap.secondstorage.test;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.Shell;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.apache.kylin.common.util.AbstractTestCase.overwriteSystemPropBeforeClass;
import static org.apache.kylin.common.util.AbstractTestCase.restoreSystemPropsOverwriteBeforeClass;

public class SharedSparkSession extends ExternalResource {

    final protected SparkConf sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
    protected SparkSession ss;

    final private Map<String, String> extraConf;

    public SharedSparkSession() {
        this(Collections.emptyMap());
    }

    public SharedSparkSession(Map<String, String> extraConf) {
        this.extraConf = extraConf;
    }

    public SparkSession getSpark() {
        return ss;
    }

    @Override
    protected void before() throws Throwable {

        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        for (Map.Entry<String, String> entry : extraConf.entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
        }

        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");

        sparkConf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY");
        sparkConf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY");
        sparkConf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED");
        sparkConf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED");
        sparkConf.set("spark.sql.legacy.timeParserPolicy", "LEGACY");
        sparkConf.set("spark.sql.parquet.mergeSchema", "true");
        sparkConf.set("spark.sql.legacy.allowNegativeScaleOfDecimal", "true");

        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @Override
    protected void after() {
        ss.close();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
        restoreSystemPropsOverwriteBeforeClass();
    }
}
