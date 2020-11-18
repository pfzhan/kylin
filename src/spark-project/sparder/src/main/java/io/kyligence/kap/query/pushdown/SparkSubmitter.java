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

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.ext.classloader.ClassLoaderUtils;
import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.spark.common.CredentialUtils;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class SparkSubmitter {
    public static final Logger logger = LoggerFactory.getLogger(SparkSubmitter.class);

    public static SparkSubmitter getInstance() {
        return Singletons.getInstance(SparkSubmitter.class);
    }

    private OverriddenSparkSession overriddenSparkSession;

    public OverriddenSparkSession overrideSparkSession(SparkSession ss) {
        this.overriddenSparkSession = new OverriddenSparkSession(ss);
        return overriddenSparkSession;
    }

    public void clearOverride() {
        this.overriddenSparkSession = null;
    }

    private SparkSession getSparkSession() {
        return overriddenSparkSession != null ? overriddenSparkSession.ss : SparderEnv.getSparkSession();
    }

    public PushdownResponse submitPushDownTask(String sql, String project) {
        if (UnitOfWork.isAlreadyInTransaction()) {
            logger.warn("execute spark job with transaction lock");
        }
        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        SparkSession ss = getSparkSession();
        CredentialUtils.wrap(ss, project);
        Pair<List<List<String>>, List<StructField>> pair = SparkSqlClient.executeSql(ss, sql, UUID.randomUUID(), project);
        return new PushdownResponse(pair.getSecond(), pair.getFirst());
    }

    public class OverriddenSparkSession implements AutoCloseable {

        private SparkSession ss;

        public OverriddenSparkSession(SparkSession ss) {
            this.ss = ss;
        }

        @Override
        public void close() throws Exception {
            SparkSubmitter.this.clearOverride();
        }
    }
}
