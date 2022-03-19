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

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.calcite.sql.parser.impl.ParseException;
import org.apache.commons.lang.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class SmartMaster {

    private final String project;
    public final AbstractContext context;

    public SmartMaster(AbstractContext proposeContext) {
        this.context = proposeContext;
        this.project = proposeContext.getProject();
    }

    public AbstractProposer getProposer(String name) {
        for (AbstractProposer proposer : getContext().getProposers().getProposerList()) {
            if (proposer.getIdentifierName().equalsIgnoreCase(name)) {
                return proposer;
            }
        }
        throw new IllegalArgumentException("Wrong proposer name: " + name);
    }

    /**
     * This method will invoke when there is no need transaction.
     */
    public void executePropose() {
        getContext().getProposers().execute();
    }

    /**
     * This method now only used for testing.
     */
    @VisibleForTesting
    public void runUtWithContext(Consumer<AbstractContext> hook) {
        runWithContext(hook);
    }

    void runWithContext(Consumer<AbstractContext> hook) {
        long start = System.currentTimeMillis();
        try {
            getContext().getProposers().execute();
            getContext().saveMetadata();
            if (hook != null) {
                hook.accept(getContext());
            }
        } catch (Exception exception) {
            recordError(exception);
        } finally {
            log.info("The whole process of {} takes {}ms", context.getIdentifier(), System.currentTimeMillis() - start);
            genDiagnoseInfo();
        }
    }

    private void recordError(Throwable throwable) {
        context.getAccelerateInfoMap().forEach((key, value) -> {
            value.getRelatedLayouts().clear();
            value.setFailedCause(throwable);
        });
    }

    enum AccStatusType {
        SUCCESS, PENDING, FAILED
    }

    private void genDiagnoseInfo() {
        if (context == null) {
            log.error("Unlikely exception without proposing context!");
            return;
        }
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
