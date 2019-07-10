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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.BadModelException;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.util.ComputedColumnEvalUtil;

public class NComputedColumnProposer extends NAbstractModelProposer {

    private static final String CC_NAME_PREFIX = "CC_AUTO_";
    private static final String DEFAULT_CC_NAME = "CC_AUTO_1";

    NComputedColumnProposer(NModelContext modelCtx) {
        super(modelCtx);
    }

    @Override
    protected void doPropose(NDataModel nDataModel) {
        LOGGER.trace("Propose computed column for model [{}]", nDataModel.getId());

        List<NDataModel> otherModels = NDataflowManager.getInstance(kylinConfig, project) //
                .listUnderliningDataModels().stream() //
                .filter(m -> !m.getUuid().equals(nDataModel.getUuid())) //
                .collect(Collectors.toList());
        otherModels.addAll(getModelContext().getSmartContext() //
                .getModelContexts().stream() //
                .filter(ctx -> ctx != getModelContext()) //
                .map(NModelContext::getTargetModel) //
                .filter(Objects::nonNull) //
                .collect(Collectors.toList()) //
        );

        // pre-init to construct join-tree
        initModel(nDataModel);
        Set<String> ccSuggestions = collectComputedColumnSuggestion(modelContext, nDataModel);
        if (ccSuggestions.isEmpty()) {
            return;
        }

        TableRef rootTable = nDataModel.getRootFactTable();
        List<ComputedColumnDesc> validCCs = Lists.newArrayList();
        for (String ccSuggestion : ccSuggestions) {
            ComputedColumnDesc ccDesc = modelContext.getUsedCC().get(ccSuggestion);

            // In general, cc expressions in the SQL statements should have been replaced in transformers,
            // however, it could not be replaced when meets some corner cases(#11411). As a result, it will
            // lead to add the same CC more than once and fail to accelerate current sql statements.
            if (ccDesc != null) {
                continue;
            }

            ccDesc = new ComputedColumnDesc();
            ccDesc.setColumnName(DEFAULT_CC_NAME);
            ccDesc.setTableIdentity(rootTable.getTableIdentity());
            ccDesc.setTableAlias(nDataModel.getRootFactTableAlias());
            ccDesc.setComment("Auto suggested from: " + ccSuggestion);
            ccDesc.setDatatype("ANY"); // resolve data type later
            ccDesc.setExpression(ccSuggestion);
            ccDesc.setInnerExpression(KapQueryUtil.massageComputedColumn(nDataModel, project, ccDesc));
            nDataModel.getComputedColumnDescs().add(ccDesc);

            boolean isValidCC = resolveCCName(ccDesc, nDataModel, otherModels);
            if (isValidCC) {
                validCCs.add(ccDesc);
                modelContext.getUsedCC().put(ccDesc.getExpression(), ccDesc);
            }
        }

        if (!modelContext.getSmartContext().isSkipEvaluateCC() && !validCCs.isEmpty()) {
            ComputedColumnEvalUtil.evaluateExprAndTypes(nDataModel, validCCs);
        }

        // there are three kind of CC need remove:
        // 1. invalid CC: something wrong happened in resolving name
        // 2. unsupported CC: something wrong happened in inferring type
        // 3. the type of CC is ANY: something unlikely thing happened in inferring type
        nDataModel.getComputedColumnDescs().removeIf(cc -> cc.getDatatype().equals("ANY"));
    }

    private Set<String> collectComputedColumnSuggestion(NModelContext modelContext, NDataModel nDataModel) {
        Set<String> ccSuggestions = Sets.newHashSet();

        ModelTree modelTree = modelContext.getModelTree();

        // Load from context
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            // fix models to update alias
            Map<String, String> matchingAlias = RealizationChooser.matchJoins(nDataModel, ctx);
            ctx.fixModel(nDataModel, matchingAlias);
            ccSuggestions.addAll(collectInnerColumnCandidate(ctx, matchingAlias));
            ctx.unfixModel();
        }

        return ccSuggestions;
    }

    private Set<String> collectInnerColumnCandidate(OLAPContext context, Map<String, String> matchingAlias) {
        Set<TblColRef> usedCols = Sets.newHashSet();
        Set<String> candidates = Sets.newHashSet();
        usedCols.addAll(context.allColumns);

        context.aggregations.stream() //
                .filter(agg -> CollectionUtils.isNotEmpty(agg.getParameters()))
                .forEach(agg -> usedCols.addAll(agg.getColRefs()));
        for (TblColRef col : usedCols) {
            if (col.isInnerColumn()) {
                String parserDesc = col.getParserDescription();
                parserDesc = matchingAlias.entrySet().stream()
                        .map(entry -> (Function<String, String>) s -> s.replaceAll(entry.getKey(), entry.getValue()))
                        .reduce(Function.identity(), Function::andThen).apply(parserDesc);
                LOGGER.trace(parserDesc);
                candidates.add(parserDesc);
            }
        }
        return candidates;
    }

    private boolean resolveCCName(ComputedColumnDesc ccDesc, NDataModel nDataModel, List<NDataModel> otherModels) {
        KylinConfig config = getModelContext().getSmartContext().getKylinConfig();
        String project = getModelContext().getSmartContext().getProject();

        // Resolve CC name, Limit 99 retries to avoid infinite loop
        int retryCount = 0;
        while (retryCount < 99) {
            retryCount++;
            try {
                // Init model to check CC availability
                nDataModel.init(config, NTableMetadataManager.getInstance(config, project).getAllTablesMap(),
                        otherModels, project);
                // No exception, check passed
                return true;
            } catch (BadModelException e) {
                switch (e.getCauseType()) {
                case SAME_NAME_DIFF_EXPR:
                case WRONG_POSITION_DUE_TO_NAME:
                case SELF_CONFLICT:
                    // updating CC auto index to resolve name conflict
                    String ccName = ccDesc.getColumnName();
                    ccDesc.setColumnName(incrementIndex(ccName));
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

    private static String incrementIndex(String oldAlias) {
        if (oldAlias == null || !oldAlias.startsWith(CC_NAME_PREFIX) || oldAlias.equals(CC_NAME_PREFIX)) {
            return DEFAULT_CC_NAME;
        }

        String idxStr = oldAlias.substring(CC_NAME_PREFIX.length());
        Integer idx;
        try {
            idx = Integer.valueOf(idxStr);
        } catch (NumberFormatException e) {
            return DEFAULT_CC_NAME;
        }

        idx++;
        return CC_NAME_PREFIX + idx.toString();
    }
}