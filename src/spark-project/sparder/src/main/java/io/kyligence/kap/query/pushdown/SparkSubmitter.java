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

package io.kyligence.kap.query.pushdown;

import io.kyligence.kap.metadata.query.StructField;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.kylin.common.util.Pair;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.ext.classloader.ClassLoaderUtils;
import io.kyligence.kap.spark.common.CredentialUtils;

public class SparkSubmitter {
    public static final Logger logger = LoggerFactory.getLogger(SparkSubmitter.class);
    private static Semaphore semaphore = new Semaphore((int) (Runtime.getRuntime().totalMemory() / (1024 * 1024)));

    public static PushdownResponse submitPushDownTask(String sql, String project) {
        if (UnitOfWork.isAlreadyInTransaction()) {
            logger.warn("execute spark job with transaction lock");
        }
        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        SparkSession ss = SparderEnv.getSparkSession();
        CredentialUtils.wrap(ss, project);
        Pair<List<List<String>>, List<StructField>> pair = SparkSqlClient.executeSql(ss, sql, UUID.randomUUID());
        return new PushdownResponse(pair.getSecond(), pair.getFirst());
    }

}
