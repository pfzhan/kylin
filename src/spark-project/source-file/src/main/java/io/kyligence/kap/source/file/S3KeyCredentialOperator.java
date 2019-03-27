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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;

import java.io.IOException;

public class S3KeyCredentialOperator extends CredentialOperator {
    private static Logger logger = LoggerFactory.getLogger(S3KeyCredentialOperator.class);

    private S3KeyCredential credential;

    public ICredential getCredential() {
        return credential;
    }

    public S3KeyCredentialOperator() {
    }

    @Override
    public CredentialOperator decode(String credential) {
        try {
            this.credential = JsonUtil.readValue(credential, S3KeyCredential.class);
        } catch (IOException e) {
            logger.error("Decode credential failed.{}", e);
            throw new IllegalStateException(e);
        }
        return this;
    }

    @Override
    public String encode() {
        try {
            return JsonUtil.writeValueAsString(this.credential);
        } catch (JsonProcessingException e) {
            logger.error("Encode credential failed.{}", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void build(SparkSession ss) {
        ss.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", credential.getAccessKey());
        ss.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", credential.getSecretKey());
    }

    @Override
    public void buildConf(SparkConf conf) {
        conf.set("spark.hadoop.fs.s3a.access.key", credential.getAccessKey());
        conf.set("spark.hadoop.fs.s3a.secret.key", credential.getSecretKey());
    }

    @Override
    public String massageUrl(String url) {
        return url.replace("s3:", "s3a:");
    }

    @Override
    public boolean verify(String url) {
        AWSCredentials credentials = new BasicAWSCredentials(credential.getAccessKey(), credential.getSecretKey());
        AmazonS3 s3 = new AmazonS3Client(credentials);
        try {
            s3.getS3AccountOwner();
        } catch (AmazonS3Exception e) {
            logger.error("Get credential failed. {}", e);
            throw new CheckCredentialException("Please check your credential", e);
        }
        String bucket = url.substring(url.indexOf("//") + 2, StringUtils.ordinalIndexOf(url, "/", 3));
        if (!s3.doesBucketExist(bucket)) {
            logger.error("Get S3 object failed ");
            throw new CheckObjectException("Please check your s3 url.");
        }
        return true;
    }
}
