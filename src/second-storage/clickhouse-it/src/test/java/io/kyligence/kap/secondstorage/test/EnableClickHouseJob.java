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

import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.newten.clickhouse.EmbeddedHttpServer;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import lombok.SneakyThrows;
import org.eclipse.jetty.toolchain.test.SimpleRequest;
import org.junit.Assert;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.IOException;

import static io.kyligence.kap.common.util.NLocalFileMetadataTestCase.getLocalWorkingDirectory;


public class EnableClickHouseJob extends EnableScheduler {

    private final JdbcDatabaseContainer<?>[] clickhouse;
    private final int replica;
    private final String modelName;
    private final SecondStorageService secondStorageService = new SecondStorageService();

    private EmbeddedHttpServer _httpServer;

    public EnableClickHouseJob(int nClickhouse, int replica, String project, String modelName, String... extraMeta) {
        super(project, extraMeta);
        clickhouse = new JdbcDatabaseContainer<?>[nClickhouse];
        this.replica = replica;
        this.modelName = modelName;
    }

    @Override
    protected void before() throws Throwable {
        super.before();

        // setup http server
        _httpServer = EmbeddedHttpServer.startServer(getLocalWorkingDirectory());
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, _httpServer.uriAccessedByDocker.toString());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        //setup clickhouse
        for (int i = 0; i < clickhouse.length; i++) {
            clickhouse[i] = ClickHouseUtils.startClickHouse();
        }
        overwriteSystemProp("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());
        ClickHouseUtils.internalConfigClickHouse(clickhouse, replica);
        secondStorageService.changeProjectSecondStorageState(project, SecondStorageNodeHelper.getAllNames(), true);
        Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(project).size());
        secondStorageService.changeModelSecondStorageState(project, modelName, true);
    }

    public JdbcDatabaseContainer<?> getClickhouse(int index) {
        Assert.assertTrue(index < clickhouse.length);
        return clickhouse[index];
    }

    @SneakyThrows
    @Override
    protected void after() {
        for (int i = 0; i < clickhouse.length; i++) {
            clickhouse[i].close();
            clickhouse[i] = null;
        }

        _httpServer.stopServer();
        super.after();
    }

    public void checkHttpServer() throws IOException {
        SimpleRequest sr = new SimpleRequest(_httpServer.serverUri);
        final String content = sr.getString("/");
        Assert.assertTrue(content.length() > 0);
    }
}
