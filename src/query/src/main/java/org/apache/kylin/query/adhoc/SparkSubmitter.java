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

package org.apache.kylin.query.adhoc;

import io.kyligence.kap.ext.classloader.ClassLoaderUtils;
import io.kyligence.kap.storage.parquet.PushdownResponse;
import io.kyligence.kap.storage.parquet.StructField;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;

public class SparkSubmitter {
    public static final Logger logger = LoggerFactory.getLogger(SparkSubmitter.class);
    private static Semaphore semaphore = new Semaphore((int) (Runtime.getRuntime().totalMemory() / (1024 * 1024)));

    public static PushdownResponse submitPushDownTask(String sql) {
        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        Pair<List<List<String>>, List<StructField>> pair = null;

        pair = new SparkSqlClient(SparkSession.builder().getOrCreate(), semaphore).executeSql(sql, UUID.randomUUID());
        return new PushdownResponse(pair.getSecond(), pair.getFirst());
    }

}
