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

package io.kyligence.kap.tool.security;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import io.kyligence.kap.common.persistence.metadata.jdbc.AuditLogRowMapper;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.val;

public class KapPasswordResetCLITest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "testKapPasswordResetCLITest@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
    }

    @After
    public void teardown() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
    }

    @Test
    public void testResetAdminPassword() throws Exception {
        val pwdEncoder = new BCryptPasswordEncoder();
        val user = new ManagedUser("ADMIN", "KYLIN", true, Constant.ROLE_ADMIN, Constant.GROUP_ALL_USERS);
        user.setPassword(pwdEncoder.encode(user.getPassword()));
        val config = KylinConfig.getInstanceFromEnv();
        val manger = NKylinUserManager.getInstance(config);
        manger.update(user);

        Assert.assertEquals(user.getPassword(), manger.get(user.getUsername()).getPassword());

        val modifyUser = manger.get(user.getUsername());
        modifyUser.setPassword(pwdEncoder.encode("KYLIN2"));
        manger.update(modifyUser);
        Assert.assertEquals(modifyUser.getPassword(), manger.get(user.getUsername()).getPassword());
        Assert.assertTrue(pwdEncoder.matches("KYLIN2", manger.get(user.getUsername()).getPassword()));

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output));

        KapPasswordResetCLI.reset();

        ResourceStore.clearCache(config);
        config.clearManagers();
        val afterManager = NKylinUserManager.getInstance(config);

        Assert.assertTrue(!pwdEncoder.matches("KYLIN", afterManager.get(user.getUsername()).getPassword()));
        Assert.assertTrue(output.toString().startsWith(StorageCleaner.ANSI_RED + "Reset password of ["
                + StorageCleaner.ANSI_RESET + "ADMIN" + StorageCleaner.ANSI_RED + "] succeed. The password is "));
        Assert.assertTrue(
                output.toString().endsWith("Please keep the password properly." + StorageCleaner.ANSI_RESET + "\n"));

        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = getJdbcTemplate();
        val all = jdbcTemplate.query("select * from " + url.getIdentifier() + "_audit_log", new AuditLogRowMapper());
        Assert.assertTrue(all.stream().anyMatch(auditLog -> auditLog.getResPath().equals("/_global/user/ADMIN")));

        System.setOut(System.out);
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
