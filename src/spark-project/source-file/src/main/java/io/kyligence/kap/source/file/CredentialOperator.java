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
package io.kyligence.kap.source.file;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CredentialOperator {
    private static Logger logger = LoggerFactory.getLogger(CredentialOperator.class);

    public abstract ICredential getCredential();

    public abstract CredentialOperator decode(String credentialString);

    public abstract String encode();

    /**
     * Wrap spark session with credential
     * @param ss
     */
    public abstract void build(SparkSession ss);

    /**
     * Wrap spark conf with credential
     * used by reflect
     * @param conf
     */
    public abstract void buildConf(SparkConf conf);

    /**
     * Util method for decode & build
     * @param credentialString
     * @param ss
     * @return
     */
    public CredentialOperator prepare(String credentialString, SparkSession ss) {
        CredentialOperator credentialOperator = decode(credentialString);
        credentialOperator.build(ss);
        return credentialOperator;
    }

    /**
     * Get first 10 row with csv configuration return as Two-Dimension array
     * @param url file url
     * @param configuration
     * @see org.apache.spark.sql.execution.datasources.csv.CSVOptions
     * @return two-dimension array
     */
    public String[][] csvSamples(String url, Map<String, String> configuration) {
        url = massageUrl(url);
        long start = System.currentTimeMillis();
        String[][] result = new String[10][];
        SparkSession sparkSession = SparderEnv.getSparkSession();
        Dataset<Row> csv = sparkSession.read().options(configuration).csv(url);
        List<Row> rows = csv.takeAsList(10);
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            result[i] = new String[row.size()];
            for (int j = 0; j < row.size(); j++) {
                result[i][j] = (String) row.get(j);
            }
        }
        long end = System.currentTimeMillis();
        logger.info("Get samples elapsed time: {} ms", end - start);
        return result;
    }

    /**
     * Infer schema of csv file
     * @param url file url
     * @param configuration
     * @see org.apache.spark.sql.execution.datasources.csv.CSVOptions
     * @return schema as a list
     */
    public List<String> csvSchema(String url, Map<String, String> configuration) {
        url = massageUrl(url);
        long start = System.currentTimeMillis();
        List<String> result = Lists.newArrayList();
        SparkSession sparkSession = SparderEnv.getSparkSession();
        Dataset<Row> tmp = sparkSession.read().options(configuration).csv(url).limit(10);
        String tmpPath = "/tmp/" + UUID.randomUUID().toString();
        tmp.write().format("csv").save(tmpPath);
        Dataset<Row> csv = sparkSession.read().option("header", "true").option("inferSchema", "true").csv(tmpPath);
        StructType structType = csv.schema();
        for (StructField structField : structType.fields()) {
            result.add(structField.dataType().typeName());
        }
        long end = System.currentTimeMillis();
        logger.info("Infer schema elapsed time: {} ms", end - start);
        return result;
    }

    /**
     * wrap file url
     * @param url
     * @return
     */
    public abstract String massageUrl(String url);

    /**
     * Verify credential operator has access to file of url or not
     * @param url
     * @return
     */
    public abstract boolean verify(String url);

    /**
     * Used by reflect
     * @param type
     * @return
     */
    public static String chooseCredentialClassName(String type) {
        switch (type) {
        case "AWS_S3_KEY":
            return S3KeyCredentialOperator.class.getCanonicalName();
        case "LOCAL":
            return LocalFileCredentialOperator.class.getCanonicalName();
        default:
            return null;
        }
    }

    public static CredentialOperator chooseCredential(String type) {
        switch (type) {
        case "AWS_S3_KEY":
            return new S3KeyCredentialOperator();
        case "LOCAL":
            return new LocalFileCredentialOperator();
        default:
            return null;
        }
    }
}
