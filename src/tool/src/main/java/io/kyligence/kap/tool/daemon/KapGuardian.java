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
package io.kyligence.kap.tool.daemon;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.kyligence.kap.tool.daemon.handler.DownGradeStateHandler;
import io.kyligence.kap.tool.daemon.handler.NormalStateHandler;
import io.kyligence.kap.tool.daemon.handler.RestartStateHandler;
import io.kyligence.kap.tool.daemon.handler.SuicideStateHandler;
import io.kyligence.kap.tool.daemon.handler.UpGradeStateHandler;
import io.kyligence.kap.tool.daemon.handler.WarnStateHandler;
import org.apache.calcite.avatica.util.Unsafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KapGuardian {
    private static final Logger logger = LoggerFactory.getLogger(KapGuardian.class);

    private List<HealthChecker> healthCheckers = Lists.newArrayList();

    private Map<CheckStateEnum, List<CheckStateHandler>> checkStateHandlersMap = new EnumMap<>(CheckStateEnum.class);

    private KylinConfig config = KylinConfig.getInstanceFromEnv();

    private ScheduledExecutorService executor;

    /**
     * uuid = KYLIN_HOME & server port
     */
    public KapGuardian() {
        if (!config.isGuardianEnabled()) {
            logger.warn("Do not enable to start Guardian Process, exit 0!");
            Unsafe.systemExit(0);
        }

        executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("ke-guardian-process").build());

        this.loadCheckers();
        this.initCheckStateHandler();
    }

    public void start() {
        executor.scheduleWithFixedDelay(() -> {
            try {
                logger.info("Guardian Process: start to run health checkers ...");
                for (HealthChecker healthChecker : healthCheckers) {
                    CheckResult checkResult = healthChecker.check();
                    List<CheckStateHandler> handlers = checkStateHandlersMap.get(checkResult.getCheckState());

                    if (CollectionUtils.isEmpty(handlers)) {
                        continue;
                    }

                    for (CheckStateHandler handler : handlers) {
                        HandleResult handleResult = handler.handle(checkResult);
                        if (HandleStateEnum.STOP_CHECK == handleResult.getHandleState()) {
                            logger.info("Guardian Process: state [{}] found, stop check!", HandleStateEnum.STOP_CHECK);
                            return;
                        }
                    }
                }

                logger.info("Guardian Process: health check finished ...");
            } catch (Exception e) {
                logger.info("Guardian Process: failed to run health check!", e);
            }
        }, config.getGuardianCheckInitDelay(), config.getGuardianCheckInterval(), TimeUnit.SECONDS);
    }

    public void stop() {
        if (null != executor && !executor.isShutdown()) {
            ExecutorServiceUtil.forceShutdown(executor);
        }
    }

    /**
     * optimize check config
     */
    public void loadCheckers() {
        String healthCheckersStr = config.getGuardianHealthCheckers();
        if (StringUtils.isNotBlank(healthCheckersStr)) {
            Map<Integer, List<HealthChecker>> checkerMap = new TreeMap<>();
            for (String healthCheckerClassName : healthCheckersStr.split(",")) {
                if (StringUtils.isNotBlank(healthCheckerClassName.trim())) {
                    HealthChecker healthChecker = (HealthChecker) ClassUtil.newInstance(healthCheckerClassName);
                    if (!checkerMap.containsKey(healthChecker.getPriority())) {
                        checkerMap.put(healthChecker.getPriority(), Lists.newArrayList());
                    }
                    checkerMap.get(healthChecker.getPriority()).add(healthChecker);
                }
            }

            healthCheckers.addAll(checkerMap.values().stream().flatMap(List::stream).collect(Collectors.toList()));
        }
    }

    public void initCheckStateHandler() {
        checkStateHandlersMap.put(CheckStateEnum.RESTART, Lists.newArrayList(new RestartStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.NORMAL, Lists.newArrayList(new NormalStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.WARN, Lists.newArrayList(new WarnStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.SUICIDE, Lists.newArrayList(new SuicideStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.QUERY_UPGRADE, Lists.newArrayList(new UpGradeStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.QUERY_DOWNGRADE, Lists.newArrayList(new DownGradeStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.OTHER, Lists.newArrayList());
    }

    public String getServerPort() {
        return config.getServerPort();
    }

    public String getKylinHome() {
        return KylinConfig.getKylinHome();
    }

    public static void main(String[] args) {
        logger.info("Guardian Process starting...");

        try {
            KapGuardian kapGuardian = new KapGuardian();
            kapGuardian.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Guardian Process of KE instance port[{}] KYLIN_HOME[{}] stopped",
                        kapGuardian.getServerPort(), kapGuardian.getKylinHome());
                kapGuardian.stop();
            }));
        } catch (Exception e) {
            logger.info("Guardian Process start failed", e);
            Unsafe.systemExit(1);
        }

        logger.info("Guardian Process started...");
    }

}
