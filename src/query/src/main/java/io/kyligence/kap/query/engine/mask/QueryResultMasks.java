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

package io.kyligence.kap.query.engine.mask;

import io.kyligence.kap.query.QueryExtension;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.common.KylinConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public final class QueryResultMasks {

    private QueryResultMasks() {
    }

    public static final ThreadLocal<QueryResultMask> THREAD_LOCAL = new ThreadLocal<>();

    public static void init(String project, KylinConfig kylinConfig) {
        QueryExtension.getFactory().getQueryResultMasksExtension().addQueryResultMasks(kylinConfig, project, THREAD_LOCAL);
    }

    public static void remove() {
        THREAD_LOCAL.remove();
    }

    public static Dataset<Row> maskResult(Dataset<Row> df) {
        if (THREAD_LOCAL.get() == null) {
            return df;
        }
        return THREAD_LOCAL.get().doMaskResult(df);
    }

    public static void setRootRelNode(RelNode relNode) {
        if (THREAD_LOCAL.get() != null) {
            THREAD_LOCAL.get().doSetRootRelNode(relNode);
        }
    }
}
