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
package io.kyligence.kap.rest;

import static io.kyligence.kap.common.util.TestUtils.getTestConfig;

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.autoconfigure.session.SessionProperties;
import org.springframework.boot.autoconfigure.session.StoreType;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.junit.annotation.MetadataInfo;
import io.kyligence.kap.junit.annotation.OverwriteProp;
import io.kyligence.kap.tool.util.MetadataUtil;

@ExtendWith(MockitoExtension.class)
@MetadataInfo(onlyProps = true)
class HAConfigurationTest {

    @InjectMocks
    HAConfiguration configuration = Mockito.spy(new HAConfiguration());

    @Mock
    SessionProperties sessionProperties;

    DataSource dataSource;

    @BeforeEach
    public void setup() throws Exception {
        dataSource = Mockito.spy(MetadataUtil.getDataSource(getTestConfig()));
        ReflectionTestUtils.setField(configuration, "dataSource", dataSource);
    }

    @Test
    @OverwriteProp(key = "kylin.metadata.url", value = "haconfigurationtest@jdbc")
    void testInitSessionTablesWithTableNonExists() throws Exception {
        Mockito.when(sessionProperties.getStoreType()).thenReturn(StoreType.JDBC);
        KylinConfig config = getTestConfig();

        String tableName = config.getMetadataUrlPrefix() + "_session_v2";
        Assertions.assertEquals("haconfigurationtest_session_v2", tableName);
        configuration.initSessionTables();
        Assertions.assertTrue(JdbcUtil.isTableExists(dataSource.getConnection(), tableName));
    }

}