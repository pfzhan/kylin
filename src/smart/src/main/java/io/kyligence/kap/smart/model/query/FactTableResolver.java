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

package io.kyligence.kap.smart.model.query;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;

public class FactTableResolver {

    private static final Logger logger = LoggerFactory.getLogger(FactTableResolver.class);

    public static Map<TableDesc, Set<String>> classifyFactTables(String projectName, String[] sqls) {

        Map<TableDesc, Set<String>> factQueryMap = new HashMap<>();

        MockupQueryExecutor queryExecutor = new MockupQueryExecutor();
        for (String sql : sqls) {
            // Prepare query context
            OLAPContext.clearThreadLocalContexts();
            queryExecutor.execute(projectName, sql);

            Collection<OLAPContext> ctxs = OLAPContext.getThreadLocalContexts();
            if (ctxs == null || ctxs.isEmpty()) {
                logger.info("Skip bad query: \n{}", sql);
                continue;
            }
            for (OLAPContext ctx : ctxs) {
                OLAPTableScan factTableScan = ctx.firstTableScan;
                if (factTableScan == null || factTableScan.getTableRef() == null) {
                    logger.warn("Cannot identify fact table");
                    continue;
                }

                TableDesc factTable = factTableScan.getTableRef().getTableDesc();
                if (!factQueryMap.containsKey(factTable)) {
                    factQueryMap.put(factTable, new HashSet<String>());
                }

                factQueryMap.get(factTable).add(sql);
            }
        }

        return factQueryMap;
    }
}
