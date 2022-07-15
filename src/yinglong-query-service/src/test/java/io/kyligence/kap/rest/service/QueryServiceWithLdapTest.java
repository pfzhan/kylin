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


package io.kyligence.kap.rest.service;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import io.kyligence.kap.common.util.EncryptUtil;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.ldap.test.unboundid.LdapTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.io.FileInputStream;
import java.util.Properties;

import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@ContextHierarchy({@ContextConfiguration(locations = {"classpath:applicationContext.xml"}),
        @ContextConfiguration(locations = {"classpath:kylinSecurity.xml"})})
@WebAppConfiguration(value = "src/main/resources")
@ActiveProfiles({"ldap", "ldap-test", "test"})
public class QueryServiceWithLdapTest extends NLocalFileMetadataTestCase {

    private static final String LDAP_CONFIG = "ut_ldap/ldap-config.properties";

    private static final String LDAP_SERVER = "ut_ldap/ldap-server.ldif";

    private static InMemoryDirectoryServer directoryServer;

    @Autowired
    private WebApplicationContext context;

    private MockMvc mockMvc;

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Autowired
    @Qualifier("userService")
    LdapUserService ldapUserService;

    @Autowired
    @Qualifier("userGroupService")
    LdapUserGroupService userGroupService;

    @Autowired
    @Qualifier("queryService")
    QueryService queryService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupResource() throws Exception {
        staticCreateTestMetadata();
        Properties ldapConfig = new Properties();
        ldapConfig.load(new FileInputStream(new ClassPathResource(LDAP_CONFIG).getFile()));
        final KylinConfig kylinConfig = getTestConfig();
        overwriteSystemPropBeforeClass("kylin.security.ldap.max-page-size", "1");
        ldapConfig.forEach((k, v) -> kylinConfig.setProperty(k.toString(), v.toString()));

        String dn = ldapConfig.getProperty("kylin.security.ldap.connection-username");
        String password = ldapConfig.getProperty("kylin.security.ldap.connection-password");
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
        config.addAdditionalBindCredentials(dn, EncryptUtil.decrypt(password));
        config.setListenerConfigs(InMemoryListenerConfig.createLDAPConfig("LDAP", 8389));
        config.setEnforceSingleStructuralObjectClass(false);
        config.setEnforceAttributeSyntaxCompliance(true);
        config.setMaxSizeLimit(1);
        directoryServer = new InMemoryDirectoryServer(config);
        directoryServer.startListening();
        log.info("current directory server listen on {}", directoryServer.getListenPort());
        LdapTestUtils.loadLdif(directoryServer, new ClassPathResource(LDAP_SERVER));
    }

    @AfterClass
    public static void cleanupResource() throws Exception {
        directoryServer.shutDown(true);
        staticCleanupTestMetadata();
    }

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).apply(springSecurity()).build();
    }

    @After
    public void cleanup() {
    }

    @Test
    public void testCollectQueryScanRowsAndTimeCondition() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            boolean isEffective;
            config.setProperty("kylin.query.auto-adjust-big-query-rows-threshold-enabled", "false");
            isEffective = queryService.isCollectQueryScanRowsAndTimeEnabled();
            Assert.assertFalse(isEffective);
            config.setProperty("kylin.query.auto-adjust-big-query-rows-threshold-enabled", "true");
            QueryContext.current().getQueryTagInfo().setAsyncQuery(true);
            isEffective = queryService.isCollectQueryScanRowsAndTimeEnabled();
            Assert.assertFalse(isEffective);
            QueryContext.current().getQueryTagInfo().setAsyncQuery(false);
            isEffective = queryService.isCollectQueryScanRowsAndTimeEnabled();
            Assert.assertTrue(isEffective);
            QueryContext.current().getQueryTagInfo().setStorageCacheUsed(true);
            isEffective = queryService.isCollectQueryScanRowsAndTimeEnabled();
            Assert.assertFalse(isEffective);
            QueryContext.current().getQueryTagInfo().setStorageCacheUsed(false);
            isEffective = queryService.isCollectQueryScanRowsAndTimeEnabled();
            Assert.assertTrue(isEffective);
        }
    }

    @Test
    public void testGetLdapUserACLInfo() {
        {
            ldapUserService.listUsers();
            ldapUserService.loadUserByUsername("jenny");
            QueryContext.AclInfo info = queryService.getExecuteAclInfo("default", "jenny");
            Assert.assertTrue(info.getGroups().contains("ROLE_ADMIN"));
        }

        {
            ldapUserService.loadUserByUsername("johnny");
            QueryContext.AclInfo info = queryService.getExecuteAclInfo("default", "johnny");
            Assert.assertFalse(info.getGroups().contains("ROLE_ADMIN"));
        }
    }

}
