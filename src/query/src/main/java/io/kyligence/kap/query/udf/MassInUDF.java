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

package io.kyligence.kap.query.udf;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.function.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.filter.MassinFilterManager;

public class MassInUDF implements IKeep {
    private static final Logger logger = LoggerFactory.getLogger(MassInUDF.class);
    private static final ConcurrentMap<String, Set<ByteArray>> FILTER_RESULT_CACHE = new ConcurrentHashMap<>();

    public boolean eval(@Parameter(name = "col") Object col, @Parameter(name = "filterTable") String filterTable) {
        Set<ByteArray> set = FILTER_RESULT_CACHE.get(filterTable);
        if (set == null) {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            KapConfig kapConfig = KapConfig.wrap(kylinConfig);
            MassinFilterManager manager = MassinFilterManager.getInstance(kylinConfig);
            try {
                set = manager.load(Functions.FilterTableType.HDFS,
                        MassinFilterManager.getResourceIdentifier(kapConfig, filterTable));
            } catch (IOException e) {
                logger.error("{}", e);
            }
        }
        return set == null ? false : set.contains(new ByteArray(col.toString().getBytes()));
    }
}
