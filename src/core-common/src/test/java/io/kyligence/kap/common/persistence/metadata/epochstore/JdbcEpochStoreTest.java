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
package io.kyligence.kap.common.persistence.metadata.epochstore;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.base.Throwables;

import io.kyligence.kap.common.persistence.metadata.Epoch;
import lombok.val;

public final class JdbcEpochStoreTest extends AbstractEpochStoreTest {

    @Before
    public void setup() {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        epochStore = getEpochStore();
    }

    @After
    public void destroy() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    @Test
    public void testExecuteWithTransaction_RollBack() {

        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());

        try {
            epochStore.executeWithTransaction(() -> {
                epochStore.insert(e1);

                //insert success
                Assert.assertEquals(epochStore.list().size(), 1);
                Assert.assertTrue(compareEpoch(e1, epochStore.list().get(0)));

                if (epochStore.list().size() == 1) {
                    throw new RuntimeException("mock transaction error");
                }

                return null;
            });

            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertEquals(Throwables.getRootCause(e).getMessage(), "mock transaction error");
            Assert.assertEquals(epochStore.list().size(), 0);
        }

    }
}
