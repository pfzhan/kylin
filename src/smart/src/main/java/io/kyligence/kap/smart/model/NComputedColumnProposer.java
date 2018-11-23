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

package io.kyligence.kap.smart.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.NJoinedFlatTable;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.BadModelException;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.model.cc.ComputedColumnAdvisor;

public class NComputedColumnProposer extends NAbstractModelProposer {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(NComputedColumnProposer.class);

    private static final String CC_NAME_PRIFIX = "CC_AUTO_";
    private static final String DEFAULT_CC_NAME = CC_NAME_PRIFIX + "1";

    public NComputedColumnProposer(NModelContext modelCtx) {
        super(modelCtx);
    }

    @Override
    protected void doPropose(NDataModel nDataModel) {
        LOGGER.trace("Propose computed column for model [{}]", nDataModel.getId());

        KylinConfig config = getModelContext().getSmartContext().getKylinConfig();
        String project = getModelContext().getSmartContext().getProject();
        List<NDataModel> otherModels = NDataModelManager.getInstance(config, project).getDataModels().stream()
                .filter(m -> !m.getName().equals(nDataModel.getName())).collect(Collectors.toList());
        otherModels.addAll(
                getModelContext().getSmartContext().getModelContexts().stream().filter(ctx -> ctx != getModelContext())
                        .map(NModelContext::getTargetModel).filter(Objects::nonNull).collect(Collectors.toList()));

        // pre-init to construct join-tree
        initModel(nDataModel);
        Set<String> ccSuggestions = collectComputedColumnSuggestion(modelContext, nDataModel);
        if (ccSuggestions.isEmpty()) {
            return;
        }

        TableRef rootTable = nDataModel.getRootFactTable();
        List<ComputedColumnDesc> validCCs = new ArrayList<>();
        for (String ccSuggestion : ccSuggestions) {
            ComputedColumnDesc ccDesc = modelContext.getSmartContext().getUsedCC().get(ccSuggestion);
            if (ccDesc == null) {
                ccDesc = new ComputedColumnDesc();
                ccDesc.setColumnName(DEFAULT_CC_NAME);
                ccDesc.setTableIdentity(rootTable.getTableIdentity());
                ccDesc.setTableAlias(nDataModel.getRootFactTableAlias());
                ccDesc.setComment("Auto suggested from: " + ccSuggestion);
                ccDesc.setDatatype("ANY"); // resolve data type later
                ccDesc.setExpression(ccSuggestion);
                ccDesc.setInnerExpression(KapQueryUtil.massageComputedColumn(nDataModel, project, ccDesc));
            }
            nDataModel.getComputedColumnDescs().add(ccDesc);

            boolean isValidCC = resolveCCName(ccDesc, nDataModel, otherModels);
            if (isValidCC) {
                validCCs.add(ccDesc);
                modelContext.getSmartContext().getUsedCC().put(ccDesc.getExpression(), ccDesc);
            } else {
                nDataModel.getComputedColumnDescs().remove(ccDesc);
            }
        }

        // evaluate CC expression types
        List<String> expressions = validCCs.stream().map(ComputedColumnDesc::getInnerExpression)
                .collect(Collectors.toList());
        String cols = StringUtils.join(expressions, ",");
        try {
            SparkSession ss = SparderEnv.getSparkSession();
            Dataset<Row> ds = NJoinedFlatTable.generateDataset(nDataModel, ss)
                    .selectExpr(expressions.stream().map(NSparkCubingUtil::convertFromDot).toArray(String[]::new));
            for (int i = 0; i < validCCs.size(); i++) {
                SqlTypeName typeName = SparderTypeUtil.convertSparkTypeToSqlType(ds.schema().fields()[i].dataType());
                validCCs.get(i).setDatatype(typeName.getName());
            }
        } catch (Exception e) {
            // Fail directly if error in validating SQL
            throw new IllegalStateException(
                    "Auto model failed to evaluate CC " + cols + ", suggested CC expression not valid.", e);
        }
    }

    private Set<String> collectComputedColumnSuggestion(NModelContext modelContext, NDataModel nDataModel) {
        Set<String> ccSuggestions = Sets.newHashSet();

        ModelTree modelTree = modelContext.getModelTree();
        String project = modelContext.getSmartContext().getProject();

        // Load from context
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            // fix models to update alias
            Map<String, String> matchingAlias = RealizationChooser.matches(nDataModel, ctx);
            ctx.fixModel(nDataModel, matchingAlias);
            ccSuggestions.addAll(collectInnerColumnCandidate(ctx, matchingAlias));
            ccSuggestions.addAll(collectSqlAdvisorCandidate(project, ctx, nDataModel));
            ctx.unfixModel();
        }

        return ccSuggestions;
    }

    private Set<String> collectInnerColumnCandidate(OLAPContext context, Map<String, String> matchingAlias) {
        Set<TblColRef> usedCols = Sets.newHashSet();
        Set<String> candidate = Sets.newHashSet();
        usedCols.addAll(context.allColumns);
        
        context.aggregations.stream().filter(agg -> agg.getParameter() != null)
                .forEach(agg -> usedCols.addAll(agg.getParameter().getColRefs()));
        for (TblColRef col : usedCols) {
            if (col.isInnerColumn()) {
                String parserDesc = col.getParserDescription();
                parserDesc = matchingAlias.entrySet().stream()
                        .map(entry -> (Function<String, String>) s -> s.replaceAll(entry.getKey(), entry.getValue()))
                        .reduce(Function.identity(), Function::andThen).apply(parserDesc);
                LOGGER.trace(parserDesc);
                candidate.add(parserDesc);
            }
        }
        return candidate;
    }
    
    private List<String> collectSqlAdvisorCandidate(String project, OLAPContext context, NDataModel nDataModel) {
        String sql = context.sql;
        ComputedColumnAdvisor advisor = new ComputedColumnAdvisor();
        return advisor.suggestCandidate(project, nDataModel, sql);
    }
    
    private boolean resolveCCName(ComputedColumnDesc ccDesc, NDataModel nDataModel, 
            List<NDataModel> otherModels) {
        KylinConfig config = getModelContext().getSmartContext().getKylinConfig();
        String project = getModelContext().getSmartContext().getProject();

        // Resolve CC name, Limit 99 retries to avoid infinite loop
        int retryCount = 0;
        while (retryCount < 99) {
            retryCount++;
            try {
                // Init model to check CC availability
                nDataModel.init(config, NTableMetadataManager.getInstance(config, project).getAllTablesMap(),
                        otherModels, false);
                // No exception, check passed
                return true;
            } catch (BadModelException e) {
                switch (e.getCauseType()) {
                case SAME_NAME_DIFF_EXPR:
                case WRONG_POSITION_DUE_TO_NAME:
                case SELF_CONFLICT:
                    // updating CC auto index to resolve name conflict
                    String ccName = ccDesc.getColumnName();
                    ccDesc.setColumnName(increateIndex(ccName));
                    break;
                case SAME_EXPR_DIFF_NAME:
                    ccDesc.setColumnName(e.getAdvise());
                    break;
                case WRONG_POSITION_DUE_TO_EXPR:
                case LOOKUP_CC_NOT_REFERENCING_ITSELF:
                    LOGGER.debug("Bad CC suggestion: {}", ccDesc.getExpression(), e);
                    retryCount = 99; // fail directly
                    break;
                default:
                    break;
                }
            } catch (Exception e) {
                LOGGER.debug("Check CC with model failed", e);
                break; // break loop
            }
        }
        return false;
    }

    private static String increateIndex(String oldAlias) {
        if (oldAlias == null || !oldAlias.startsWith(CC_NAME_PRIFIX) || oldAlias.equals(CC_NAME_PRIFIX)) {
            return DEFAULT_CC_NAME;
        }

        String idxStr = oldAlias.substring(CC_NAME_PRIFIX.length());
        Integer idx;
        try {
            idx = Integer.valueOf(idxStr);
        } catch (NumberFormatException e) {
            return DEFAULT_CC_NAME;
        }

        idx++;
        return CC_NAME_PRIFIX + idx.toString();
    }
}
