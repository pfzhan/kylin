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

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.autoconfigure.session.SessionProperties;
import org.springframework.boot.autoconfigure.session.StoreType;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class HAConfigurationTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    HAConfiguration configuration = Mockito.spy(new HAConfiguration());

    @Mock
    SessionProperties sessionProperties;

    @Mock
    DataSource dataSource;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testInitSessionTablesWithTableNonExists() throws Exception {
        Mockito.when(sessionProperties.getStoreType()).thenReturn(StoreType.JDBC);
        Mockito.doNothing().when(configuration).initSessionTable(Mockito.anyString(), Mockito.anyString());
        try (MockedStatic<JdbcUtil> utilities = Mockito.mockStatic(JdbcUtil.class)) {
            utilities.when(() -> JdbcUtil.isTableExists(Mockito.any(), Mockito.any())).thenReturn(false);
            utilities.when(() -> JdbcUtil.isColumnExists(Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(false);
            configuration.initSessionTables();
            Mockito.verify(configuration, Mockito.never()).dropSessionTable(Mockito.any(), Mockito.anyString());
            Mockito.verify(configuration, Mockito.times(2)).initSessionTable(Mockito.anyString(), Mockito.anyString());
        }
    }

    @Test
    public void testInitSessionTablesWithTableExists() throws Exception {
        Mockito.when(sessionProperties.getStoreType()).thenReturn(StoreType.JDBC);
        try (MockedStatic<JdbcUtil> utilities = Mockito.mockStatic(JdbcUtil.class)) {
            utilities.when(() -> JdbcUtil.isTableExists(Mockito.any(), Mockito.any())).thenReturn(true);
            utilities.when(() -> JdbcUtil.isColumnExists(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
            configuration.initSessionTables();
            Mockito.verify(configuration, Mockito.never()).dropSessionTable(Mockito.any(), Mockito.anyString());
            Mockito.verify(configuration, Mockito.never()).initSessionTable(Mockito.anyString(), Mockito.anyString());
        }
    }

    @Test
    public void testInitSessionTablesWithPrimaryNonExists() throws Exception {
        Mockito.when(sessionProperties.getStoreType()).thenReturn(StoreType.JDBC);
        Mockito.doNothing().when(configuration).dropSessionTable(Mockito.any(), Mockito.anyString());
        Mockito.doNothing().when(configuration).initSessionTable(Mockito.anyString(), Mockito.anyString());
        try (MockedStatic<JdbcUtil> utilities = Mockito.mockStatic(JdbcUtil.class)) {
            utilities.when(() -> JdbcUtil.isTableExists(Mockito.any(), Mockito.any())).thenReturn(true)
                    .thenReturn(false);
            utilities.when(() -> JdbcUtil.isColumnExists(Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(false);
            configuration.initSessionTables();
            Mockito.verify(configuration, Mockito.times(2)).dropSessionTable(Mockito.any(), Mockito.anyString());
            Mockito.verify(configuration, Mockito.times(2)).initSessionTable(Mockito.anyString(), Mockito.anyString());
        }
    }
}