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

package io.kyligence.kap.smart.query.validator;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc.TableKind;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.base.Preconditions;

import io.kyligence.kap.smart.model.ModelContext;
import io.kyligence.kap.smart.model.ModelContextBuilder;
import io.kyligence.kap.smart.model.ModelMaster;
import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerFactory;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.advisor.ISQLAdvisor;
import io.kyligence.kap.smart.query.advisor.RawModelSQLAdvisor;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import io.kyligence.kap.smart.util.JoinDescUtil;

public class RawModelSQLValidator extends AbstractSQLValidator {

    private String projectName;
    private ISQLAdvisor sqlAdvisor;
    private TableDesc factTable;
    private ModelContext validatedContext;

    public RawModelSQLValidator(KylinConfig kylinConfig, String project, String factTableName) {
        super(kylinConfig);
        this.projectName = project;
        this.factTable = MetadataManager.getInstance(kylinConfig).getTableDesc(factTableName, project);
        Preconditions.checkArgument(factTable != null, "Fact table not found: " + factTableName);
        this.sqlAdvisor = new RawModelSQLAdvisor(this.factTable);
    }

    @Override
    protected ISQLAdvisor getSQLAdvisor() {
        return sqlAdvisor;
    }

    @Override
    protected AbstractQueryRunner createQueryRunner(String[] sqls) {
        return QueryRunnerFactory.createForModelSuggestion(kylinConfig, sqls, threadCount, projectName);
    }

    @Override
    protected Map<String, SQLValidateResult> doBatchValidate(List<String> sqlList, Map<String, SQLResult> queryResults,
            Map<String, Collection<OLAPContext>> olapContexts) {
        ModelContextBuilder contextBuilder = new ModelContextBuilder(kylinConfig, projectName);
        Map<TableDesc, ModelContext> contextMap = contextBuilder.buildFromOLAPContexts(olapContexts);
        Map<String, SQLValidateResult> validationResults = super.doBatchValidate(sqlList, queryResults, olapContexts);
        doValidateJoins(contextMap.get(factTable), sqlList, validationResults);

        Map<String, Collection<OLAPContext>> capableOLAPContext = new HashMap<>();
        for (Map.Entry<String, Collection<OLAPContext>> olapContext : olapContexts.entrySet()) {
            String sql = olapContext.getKey();
            if (!validationResults.containsKey(sql)) {
                continue;
            }
            SQLValidateResult result = validationResults.get(sql);
            if (result.isCapable()) {
                capableOLAPContext.put(sql, olapContext.getValue());
            }
        }

        Map<TableDesc, ModelContext> capableContextMap = contextBuilder.buildFromOLAPContexts(capableOLAPContext);
        validatedContext = capableContextMap.get(factTable);

        return validationResults;

    }

    private void doValidateJoins(ModelContext context, List<String> sqlList,
            Map<String, SQLValidateResult> validationResults) {
        if (context == null) {
            return;
        }

        Map<String, JoinTableDesc> joinTables = new HashMap<>();
        Map<TableRef, String> tableAliasMap = context.getAllTableRefAlias();

        for (String sql : sqlList) {
            Collection<OLAPContext> olapContexts = context.getOLAPContext(sql);
            if (null == olapContexts || olapContexts.isEmpty()) {
                continue;
            }

            // Save context updates and apply later
            Map<String, JoinTableDesc> joinTablesModification = new HashMap<>();
            boolean skipModification = false;

            for (OLAPContext ctx : olapContexts) {
                if (ctx == null || ctx.joins == null || ctx.joins.size() == 0) {
                    continue;
                }

                for (JoinDesc join : ctx.joins) {
                    String pkTblAlias = tableAliasMap.get(join.getPKSide());
                    String fkTblAlias = tableAliasMap.get(join.getFKSide());

                    JoinTableDesc joinTable = JoinDescUtil.convert(join, TableKind.LOOKUP, pkTblAlias, fkTblAlias);

                    String joinTableAlias = joinTable.getAlias();
                    JoinTableDesc oldJoinTable = joinTables.get(joinTableAlias);
                    if (oldJoinTable == null) {
                        oldJoinTable = joinTablesModification.get(joinTableAlias);
                    }
                    if (oldJoinTable == null) {
                        joinTablesModification.put(joinTableAlias, joinTable);
                        continue;
                    }

                    // duplication check
                    if (oldJoinTable.equals(joinTable)) {
                        continue;
                    }
                    // conflict check
                    if (!JoinDescUtil.isJoinKeysEqual(oldJoinTable.getJoin(), joinTable.getJoin())) {
                        // add and resolve alias
                        String newAlias = getNewAlias(tableAliasMap.values(), join.getPKSide().getTableName());
                        joinTable.setAlias(newAlias);
                        JoinTableDesc newJoinTable = JoinDescUtil.convert(join, TableKind.LOOKUP, newAlias, fkTblAlias);
                        joinTablesModification.put(newAlias, newJoinTable);
                        tableAliasMap.put(join.getPKSide(), newAlias);
                        continue;
                    }
                    if (!JoinDescUtil.isJoinTypeEqual(oldJoinTable.getJoin(), joinTable.getJoin())) {
                        // join conflict inner <-> left, log result
                        SQLValidateResult result = validationResults.get(sql);
                        if (result == null) {
                            result = new SQLValidateResult();
                        }
                        result.setCapable(false);
                        Set<SQLAdvice> advice = result.getSQLAdvices();
                        String reason = "[" + JoinDescUtil.toString(joinTable) + "] is incapable with ["
                                + JoinDescUtil.toString(oldJoinTable) + "]";
                        String suggest = "Use the same join type";
                        advice.add(SQLAdvice.build(reason, suggest));
                        result.setSQLAdvices(advice);
                        validationResults.put(sql, result);
                        skipModification = true;
                        break;
                    }
                }
            }
            if (skipModification) {
                break;
            }
            joinTables.putAll(joinTablesModification);
        }
    }

    private static String getNewAlias(Collection<String> aliasSet, String oldAlias) {
        String newAlias = oldAlias;
        int i = 1;
        while (aliasSet.contains(newAlias)) {
            newAlias = oldAlias + "_" + i;
            i++;
        }
        return newAlias;
    }

    @Override
    public Map<String, SQLValidateResult> batchValidate(List<String> sqlList) {
        // TODO Auto-generated method stub
        return super.batchValidate(sqlList);
    }

    public ModelMaster buildValidatedModelMaster() {
        if (validatedContext == null) {
            throw new IllegalStateException("No model context avaliable, run validation process first.");
        }
        return new ModelMaster(validatedContext);
    }
}
