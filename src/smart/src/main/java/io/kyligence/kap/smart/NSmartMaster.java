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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.calcite.sql.parser.impl.ParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NSmartMaster {

    public static AbstractContext proposeForAutoMode(KylinConfig config, String project, String[] sqls,
            Consumer<AbstractContext> hook) {
        AbstractContext context = new NSmartContext(config, project, sqls);
        new NSmartMaster(context).runWithContext(hook);
        return context;
    }

    public static AbstractSemiContextV2 proposeNewModelForSemiMode(KylinConfig config, String project, String[] sqls,
            Consumer<AbstractContext> hook) {
        AbstractSemiContextV2 context = new ModelCreateContextOfSemiV2(config, project, sqls);
        new NSmartMaster(context).runWithContext(hook);
        return context;
    }

    public static AbstractSemiContextV2 genOptRecommendationSemiV2(KylinConfig config, String project, String[] sqls,
            Consumer<AbstractContext> hook) {
        AbstractSemiContextV2 contextV2 = new ModelReuseContextOfSemiV2(config, project, sqls);
        new NSmartMaster(contextV2).runWithContext(hook);
        return contextV2;
    }

    public static AbstractContext selectExistedModel(KylinConfig config, String project, String[] sqls,
            Consumer<AbstractContext> hook) {
        AbstractContext context = new ModelSelectContextOfSemiV2(config, project, sqls);
        new NSmartMaster(context).runWithContext(hook);
        return context;
    }

    @Getter
    private final AbstractContext context;
    private final NProposerProvider proposerProvider;
    private final String project;

    public NSmartMaster(AbstractContext proposeContext) {
        this.context = proposeContext;
        this.project = proposeContext.getProject();
        this.proposerProvider = NProposerProvider.create(context);
    }

    @VisibleForTesting
    public void analyzeSQLs() {
        long start = System.currentTimeMillis();
        log.info("Start sql analysis.");
        proposerProvider.getSQLAnalysisProposer().execute();
        val nums = getAccelerationNumMap();
        log.info("SQL analysis completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                System.currentTimeMillis() - start, nums.get(AccStatusType.SUCCESS), nums.get(AccStatusType.PENDING),
                nums.get(AccStatusType.FAILED));
    }

    @VisibleForTesting
    public void selectModel() {
        long start = System.currentTimeMillis();
        log.info("Start model selection.");
        proposerProvider.getModelSelectProposer().execute();
        val nums = getAccelerationNumMap();
        log.info("Model selection completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                System.currentTimeMillis() - start, nums.get(AccStatusType.SUCCESS), nums.get(AccStatusType.PENDING),
                nums.get(AccStatusType.FAILED));
    }

    @VisibleForTesting
    public void optimizeModel() {
        long start = System.currentTimeMillis();
        log.info("Start model optimization.");
        proposerProvider.getModelOptProposer().execute();
        val nums = getAccelerationNumMap();
        log.info("Model optimization completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                System.currentTimeMillis() - start, nums.get(AccStatusType.SUCCESS), nums.get(AccStatusType.PENDING),
                nums.get(AccStatusType.FAILED));
    }

    @VisibleForTesting
    public void selectIndexPlan() {
        long start = System.currentTimeMillis();
        log.info("Start indexPlan selection.");
        proposerProvider.getIndexPlanSelectProposer().execute();
        val nums = getAccelerationNumMap();
        log.info("IndexPlan selection completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                System.currentTimeMillis() - start, nums.get(AccStatusType.SUCCESS), nums.get(AccStatusType.PENDING),
                nums.get(AccStatusType.FAILED));
    }

    @VisibleForTesting
    public void optimizeIndexPlan() {
        long start = System.currentTimeMillis();
        log.info("Start indexPlan optimization.");
        proposerProvider.getIndexPlanOptProposer().execute();
        val nums = getAccelerationNumMap();
        log.info("IndexPlan optimization completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                System.currentTimeMillis() - start, nums.get(AccStatusType.SUCCESS), nums.get(AccStatusType.PENDING),
                nums.get(AccStatusType.FAILED));
    }

    public void shrinkIndexPlan() {
        proposerProvider.getIndexPlanShrinkProposer().execute();
    }

    public void shrinkModel() {
        proposerProvider.getModelShrinkProposer().execute();
    }

    /**
     * This method will invoke when there is no need transaction.
     */
    public void executePropose() {
        preProcessWithoutTransaction();
        processWithTransaction();
    }

    private void preProcessWithoutTransaction() {
        getContext().getPreProcessProposers().execute();
    }

    private void processWithTransaction() {
        getContext().getProcessProposers().execute();
    }

    public void runSuggestModel() {
        executePropose();
    }

    public List<NDataModel> getRecommendedModels() {
        if (CollectionUtils.isEmpty(getContext().getModelContexts())) {
            return Lists.newArrayList();
        }

        List<NDataModel> models = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : getContext().getModelContexts()) {
            NDataModel model = modelContext.getTargetModel();
            if (model == null)
                continue;

            models.add(modelContext.getTargetModel());
        }

        return models;
    }

    /**
     * This method now only used for testing.
     */
    @VisibleForTesting
    public void runUtWithContext(Consumer<AbstractContext> hook) {
        runWithContext(hook);
    }

    private void runWithContext(Consumer<AbstractContext> hook) {
        long start = System.currentTimeMillis();
        try {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(new UnitOfWork.Callback<Object>() {
                @Override
                public void preProcess() {
                    preProcessWithoutTransaction();
                }

                @Override
                public Object process() {
                    processWithTransaction();
                    getContext().saveMetadata();
                    if (hook != null) {
                        hook.accept(getContext());
                    }
                    return null;
                }

                @Override
                public void onProcessError(Throwable throwable) {
                    recordError(throwable);
                }
            }, project);
        } finally {
            log.info("The whole process of {} takes {}ms", context.getIdentifier(), System.currentTimeMillis() - start);
            saveAccelerationInfoInTransaction();
        }
    }

    private void saveAccelerationInfoInTransaction() {
        try {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                genDiagnoseInfo();
                return null;
            }, project);
        } catch (Exception e) {
            log.error("save acceleration info error", e);
        }
    }

    private void recordError(Throwable throwable) {
        context.getAccelerateInfoMap().forEach((key, value) -> {
            value.getRelatedLayouts().clear();
            value.setFailedCause(throwable);
        });
    }

    private Map<AccStatusType, Integer> getAccelerationNumMap() {
        Map<AccStatusType, Integer> result = Maps.newHashMap();
        result.putIfAbsent(AccStatusType.SUCCESS, 0);
        result.putIfAbsent(AccStatusType.PENDING, 0);
        result.putIfAbsent(AccStatusType.FAILED, 0);
        val accelerateInfoMap = context.getAccelerateInfoMap();
        for (Map.Entry<String, AccelerateInfo> entry : accelerateInfoMap.entrySet()) {
            if (entry.getValue().isPending()) {
                result.computeIfPresent(AccStatusType.PENDING, (k, v) -> v + 1);
            } else if (entry.getValue().isFailed()) {
                result.computeIfPresent(AccStatusType.FAILED, (k, v) -> v + 1);
            } else {
                result.computeIfPresent(AccStatusType.SUCCESS, (k, v) -> v + 1);
            }
        }
        return result;
    }

    enum AccStatusType {
        SUCCESS, PENDING, FAILED
    }

    private void genDiagnoseInfo() {
        Map<String, AccelerateInfo> accelerationMap = context.getAccelerateInfoMap();
        Map<String, Set<String>> failureMap = Maps.newHashMap();
        int pendingNum = 0;
        for (Map.Entry<String, AccelerateInfo> entry : accelerationMap.entrySet()) {
            if (!entry.getValue().isNotSucceed()) {
                continue;
            }
            if (entry.getValue().isPending()) {
                pendingNum++;
            }

            String expr;
            if (entry.getValue().getFailedCause() != null) {
                Throwable rootCause = Throwables.getRootCause(entry.getValue().getFailedCause());
                final String stackTraces = StringUtils.join(rootCause.getStackTrace(), "\n");
                if (rootCause instanceof ParseException) {
                    expr = "\nRoot cause: " + rootCause.getMessage().split("\n")[0] + "\n" + stackTraces;
                } else {
                    expr = "\nRoot cause: " + rootCause.getMessage() + "\n" + stackTraces;
                }
            } else {
                expr = "\nPending message: " + entry.getValue().getPendingMsg();
            }
            if (failureMap.get(expr) == null) {
                failureMap.putIfAbsent(expr, Sets.newHashSet());
            }
            failureMap.get(expr).add(entry.getKey());
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\n================== diagnosis log for auto-modeling ====================\n");
        sb.append("This round accelerates ").append(accelerationMap.size()).append(" queries.\n");
        if (failureMap.isEmpty()) {
            sb.append("No exceptions occurred.");
            sb.append("\n=======================================================================");
            log.info(sb.toString());
        } else {
            int failedNum = failureMap.values().stream().map(Set::size).reduce(Integer::sum).orElse(-1);
            sb.append("SUCCESS: ").append(accelerationMap.size() - failedNum);

            if (pendingNum != 0) {
                sb.append(", PENDING: ").append(pendingNum);
                sb.append(", FAILED: ").append(failedNum - pendingNum);
            } else {
                sb.append(", FAILED: ").append(failedNum);
            }
            sb.append(".\nClassified details are as follows:");
            failureMap.forEach((failedTypeInfo, sqlSet) -> {
                sb.append("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
                sb.append(failedTypeInfo).append("\n----------------\n");
                sb.append(String.join("\n----------------\n", sqlSet));
            });
            sb.append("\n=======================================================================");
            log.error(sb.toString());
        }
    }
}
