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

package io.kyligence.kap.clickhouse.tool;

import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NamedThreadFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class ClickHouseSanityCheckTool implements IKeep {

    public static void main(String[] args) throws InterruptedException {
        execute(args);
    }

    public static void execute(String[] args) throws InterruptedException {
        log.info("{}", args);
        SecondStorage.init(true);
        int threadNum = Integer.parseInt(args[0]);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new NamedThreadFactory("CLICKHOUSE-SANITY-CHECK"));
        val nodes = SecondStorageNodeHelper.getAllNames();
        List<Future<Boolean>> results = nodes.stream().map(node -> {
            val tool = new CheckTool(node);
            return executor.submit(tool);
        }).collect(Collectors.toList());
        List<String> failedNodes = new ArrayList<>();
        val it = results.listIterator();
        while (it.hasNext()) {
            val idx = it.nextIndex();
            val result = it.next();
            try {
                if (!result.get()) {
                    failedNodes.add(nodes.get(idx));
                }
            } catch (ExecutionException e) {
                failedNodes.add(nodes.get(idx));
            }
        }
        if (failedNodes.isEmpty()) {
            exit(0);
        } else {
            log.error("Nodes {} connect failed. Please check ClickHouse status", failedNodes);
            exit(1);
        }

    }

    public static void exit(int status) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            Unsafe.systemExit(status);
        }
    }


    public static class CheckTool implements Callable<Boolean> {
        private final String node;

        public CheckTool(final String node) {
            this.node = node;
        }

        private Boolean checkSingleNode(String node) {
            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node))) {
                clickHouse.connect();
                log.info("node {} connect success", node);
            } catch (SQLException e) {
                log.error("node {} connect failed", node, e);
                return false;
            }
            return true;
        }

        @Override
        public Boolean call() throws Exception {
            return checkSingleNode(node);
        }
    }

}
