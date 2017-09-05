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

package io.kyligence.kap.rest.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.kylin.query.security.QueryIntercept;
import org.apache.kylin.query.security.QueryInterceptUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.acl.ColumnACLManager;

public class ColumnIntercept implements QueryIntercept {
    private static final Logger logger = LoggerFactory.getLogger(QueryIntercept.class);

    @Override
    public void intercept(String project, String username, List<OLAPContext> contexts) {
        List<String> userColumnBlackList = getColumnBlackList(project, username);
        if (userColumnBlackList.isEmpty()) {
            return;
        }
        Set<String> queryCols = getQueryIdentifiers(contexts);
        for (String col : userColumnBlackList) {
            if (queryCols.contains(col.toUpperCase())) {
                throw new AccessDeniedException("column:" + col);
            }
        }
    }

    @Override
    public Set<String> getQueryIdentifiers(List<OLAPContext> contexts) {
        String project = contexts.get(0).olapSchema.getProjectName();
        return QueryInterceptUtil.getAllColsWithTblAndSchema(project, contexts);
    }

    private List<String> getColumnBlackList(String project, String username) {
        List<String> columnBlackList = new ArrayList<>();
        try {
            columnBlackList = ColumnACLManager.getInstance(KylinConfig.getInstanceFromEnv()).getColumnACL(project)
                    .getColumnBlackListByUser(username);
        } catch (IOException e) {
            logger.error("get table black list fail. " + e.getMessage());
        }
        return columnBlackList;
    }
}
