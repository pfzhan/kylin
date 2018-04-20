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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.badquery.BadQueryEntry;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.badquery.NBadQueryHistory;
import io.kyligence.kap.metadata.badquery.NBadQueryHistoryManager;

public class NSmartController {
    public static synchronized void optimizeFromPushdown(KylinConfig kylinConfig, String project) throws IOException {
        NBadQueryHistoryManager bdMgr = NBadQueryHistoryManager.getInstance(kylinConfig, project);
        NBadQueryHistory bds = bdMgr.getBadQueriesForProject();
        Set<BadQueryEntry> entries = bds.getEntries();
        Set<BadQueryEntry> toOptimize = Sets.newHashSetWithExpectedSize(entries.size());

        int i = 0;
        List<String> sqls = new ArrayList<>(entries.size());
        for (BadQueryEntry entry : entries) {
            if (!StringUtils.equals(entry.getStatus(), BadQueryEntry.STATUS_OPTIMIZED)) {
                sqls.add(entry.getSql());
                toOptimize.add(entry);
            }
        }

        NSmartMaster master = new NSmartMaster(kylinConfig, project, sqls.toArray(new String[0]));
        master.runAll();

        for (BadQueryEntry entry : toOptimize) {
            entry.setStatus(BadQueryEntry.STATUS_OPTIMIZED);
        }
        bdMgr.upsertEntryToProject(toOptimize);
    }
}
