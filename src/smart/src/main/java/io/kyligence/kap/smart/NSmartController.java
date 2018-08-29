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

package io.kyligence.kap.smart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryManager;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Sets;

public class NSmartController {
    public static synchronized void optimizeFromPushdown(KylinConfig kylinConfig, String project) throws IOException {
        QueryHistoryManager manager = QueryHistoryManager.getInstance(kylinConfig, project);
        List<QueryHistory> entries = manager.getAllQueryHistories();
        Set<QueryHistory> toOptimize = Sets.newHashSetWithExpectedSize(entries.size());

        List<String> sqls = new ArrayList<>(entries.size());
        for (QueryHistory entry : entries) {
            // TODO
            if (!StringUtils.equals(entry.getAccelerateStatus(), "FULLY_ACCELERATED") &&
                    StringUtils.equals(entry.getRealization(), QueryHistory.ADJ_PUSHDOWN)) {
                sqls.add(entry.getSql());
                toOptimize.add(entry);
            }
        }

        NSmartMaster master = new NSmartMaster(kylinConfig, project, sqls.toArray(new String[0]));
        master.runAll();

        for (QueryHistory entry : toOptimize) {
            entry.setAccelerateStatus("FULLY_ACCELERATED");
        }

        manager.upsertEntries(toOptimize);
    }
}
