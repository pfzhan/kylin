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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

public class SqlParamsFinder {

    private final static Cache<SqlCall, Map<Integer, List<Integer>>> PATH_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS).maximumSize(100).build();

    private Map<Integer, List<Integer>> paramPath;

    private SqlCall sourceTmpl;

    private SqlCall sqlCall;

    public SqlParamsFinder(SqlCall sourceTmpl, SqlCall sqlCall) {
        this.sourceTmpl = sourceTmpl;
        this.sqlCall = sqlCall;
    }

    public Map<Integer, SqlNode> getParamNodes() {
        paramPath = SqlParamsFinder.PATH_CACHE.getIfPresent(this.sourceTmpl);
        if (paramPath == null) {
            this.paramPath = new TreeMap<>();
            genParamPath(this.sourceTmpl, new ArrayList<Integer>());
            SqlParamsFinder.PATH_CACHE.put(this.sourceTmpl, this.paramPath);
        }
        Map<Integer, SqlNode> sqlNodes = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : paramPath.entrySet()) {
            List<Integer> path = entry.getValue();
            sqlNodes.put(entry.getKey(), getParamNode(path, sqlCall, 0));
        }
        return sqlNodes;
    }

    private SqlNode getParamNode(List<Integer> path, SqlNode sqlNode, int level) {
        if (level == path.size() - 1) {
            return ((SqlCall) sqlNode).getOperandList().get(path.get(level));
        } else {
            return getParamNode(path, ((SqlCall) sqlNode).getOperandList().get(path.get(level)), ++level);
        }
    }

    private void genParamPath(SqlNode sqlNode, List<Integer> path) {
        if (sqlNode instanceof SqlIdentifier) {
            int paramIdx = ParamNodeParser.parseParamIdx(sqlNode.toString());
            if (paramIdx >= 0 && path.size() > 0) {
                paramPath.put(paramIdx, path);
            }
        } else if (sqlNode instanceof SqlCall) {
            List<SqlNode> operands = ((SqlCall) sqlNode).getOperandList();
            for (int i = 0; i < operands.size(); i++) {
                List<Integer> copiedPath = Lists.newArrayList(path);
                copiedPath.add(i);
                genParamPath(operands.get(i), copiedPath);
            }
        }
    }

    public static SqlParamsFinder newInstance(SqlCall sourceTmpl, final SqlCall sqlCall, boolean isWindowCall) {
        if (!isWindowCall) {
            return new SqlParamsFinder(sourceTmpl, sqlCall);
        } else {
            return new SqlParamsFinder(sourceTmpl, sqlCall) {

                @Override
                public Map<Integer, SqlNode> getParamNodes() {
                    Map<Integer, SqlNode> sqlNodes = new HashMap<>();
                    List<SqlNode> sqlNodeList = sqlCall.getOperandList();
                    SqlNode firstParam = ((SqlCall) sqlNodeList.get(0)).getOperandList().get(0);
                    SqlNode secondParam = (SqlCall) sqlNodeList.get(1);
                    sqlNodes.put(0, firstParam);
                    sqlNodes.put(1, secondParam);
                    return sqlNodes;
                }
            };
        }
    }
}
