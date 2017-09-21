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

import java.util.List;
import java.util.Set;

import io.kyligence.kap.common.obf.IKeep;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.security.QueryIntercept;
import org.apache.kylin.query.security.QueryInterceptUtil;

import io.kyligence.kap.metadata.acl.ColumnACLManager;

public class ColumnIntercept extends QueryIntercept implements IKeep {

    @Override
    protected boolean isEnabled() {
        return KapConfig.getInstanceFromEnv().isColumnACLEnabled();
    }

    @Override
    public Set<String> getQueryIdentifiers(List<OLAPContext> contexts) {
        String project = getProject(contexts);
        return QueryInterceptUtil.getAllColsWithTblAndSchema(project, contexts);
    }

    @Override
    protected Set<String> getIdentifierBlackList(List<OLAPContext> contexts) {
        String project = getProject(contexts);
        String username = getUser(contexts);

        return ColumnACLManager
                .getInstance(KylinConfig.getInstanceFromEnv())
                .getColumnACLByCache(project)
                .getColumnBlackListByUser(username);
    }

    @Override
    protected String getIdentifierType() {
        return "column";
    }
}
