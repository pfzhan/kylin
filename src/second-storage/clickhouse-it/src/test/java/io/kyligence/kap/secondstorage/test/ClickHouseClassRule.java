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

package io.kyligence.kap.secondstorage.test;

import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.stream.IntStream;

public class ClickHouseClassRule extends ExternalResource {
    private final JdbcDatabaseContainer<?>[] clickhouse;
    public static int[] EXPOSED_PORTS = IntStream.range(24000, 25000).toArray();

    static {
        Testcontainers.exposeHostPorts(EXPOSED_PORTS);
    }

    public static int getAvailablePort() {
        synchronized (ClickHouseClassRule.class) {
            for (int port : EXPOSED_PORTS) {
                try {
                    ServerSocket s = new ServerSocket(port);
                    s.close();
                    return port;
                } catch (IOException e) {
                    // continue another port
                }
            }
        }
        throw new IllegalStateException("no available port found");
    }

    @SneakyThrows
    public ClickHouseClassRule(int n) {
        clickhouse = new JdbcDatabaseContainer<?>[n];
    }

    public JdbcDatabaseContainer<?>[] getClickhouse() {
        return clickhouse;
    }

    public JdbcDatabaseContainer<?> getClickhouse(int index) {
        Assert.assertTrue(index < clickhouse.length);
        return clickhouse[index];
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        //setup clickhouse
        for (int i = 0; i < clickhouse.length; i++) {
            clickhouse[i] = ClickHouseUtils.startClickHouse();
        }

    }

    @Override
    protected void after() {
        super.after();
        for (int i = 0; i < clickhouse.length; i++) {
            clickhouse[i].close();
            clickhouse[i] = null;
        }
    }
}
