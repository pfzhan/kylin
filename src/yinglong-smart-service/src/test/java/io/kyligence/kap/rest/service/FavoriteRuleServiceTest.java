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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.response.ImportSqlResponse;
import io.kyligence.kap.rest.response.SQLParserResponse;
import lombok.val;
import lombok.var;

public class FavoriteRuleServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @InjectMocks
    private final FavoriteRuleService favoriteRuleService = Mockito.spy(new FavoriteRuleService());

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Before
    public void setUp() {
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(favoriteRuleService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(favoriteRuleService, "userGroupService", userGroupService);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testLoadSqls() throws IOException {
        // import multiple files
        MockMultipartFile file1 = new MockMultipartFile("sqls1.sql", "sqls1.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls1.sql")));
        MockMultipartFile file2 = new MockMultipartFile("sqls2.txt", "sqls2.txt", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls2.txt")));
        // add jdbc type sql
        MockMultipartFile file3 = new MockMultipartFile("sqls3.txt", "sqls3.txt", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls3.txt")));
        MockMultipartFile exceptionFile = new MockMultipartFile("exception_file.sql", "exception_file.sql",
                "text/plain", "".getBytes(StandardCharsets.UTF_8));
        MockMultipartFile errorFile = new MockMultipartFile("error_file.sql", "error_file.sql", "text/plain",
                "".getBytes(StandardCharsets.UTF_8));

        Mockito.when(favoriteRuleService.transformFileToSqls(exceptionFile, PROJECT)).thenThrow(IOException.class);
        Mockito.when(favoriteRuleService.transformFileToSqls(errorFile, PROJECT)).thenThrow(Error.class);

        SQLParserResponse result = favoriteRuleService
                .importSqls(new MultipartFile[] { file1, file2, file3, exceptionFile, errorFile }, PROJECT);
        List<ImportSqlResponse> responses = result.getData();
        Assert.assertEquals(10, responses.size());
        Assert.assertFalse(responses.get(0).isCapable());
        Assert.assertTrue(responses.get(8).isCapable());
        Assert.assertEquals(10, result.getSize());
        Assert.assertEquals(3, result.getCapableSqlNum());
        List<String> failedFilesMsg = result.getWrongFormatFile();
        Assert.assertEquals(2, failedFilesMsg.size());
        Assert.assertEquals("exception_file.sql", failedFilesMsg.get(0));
        Assert.assertEquals("error_file.sql", failedFilesMsg.get(1));
        // import empty file
        MockMultipartFile emptyFile = new MockMultipartFile("empty_file.sql", "empty_file.sql", "text/plain",
                "".getBytes(StandardCharsets.UTF_8));
        result = favoriteRuleService.importSqls(new MultipartFile[] { emptyFile }, PROJECT);
        Assert.assertNotNull(result);
        Assert.assertEquals(0, result.getSize());
    }

    @Test
    public void testTransformFileToSqls() throws IOException {
        List<String> sqls1 = favoriteRuleService.transformFileToSqls(new MockMultipartFile("sqls5.sql", "sqls5.sql",
                "text/plain", new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls5.sql"))), PROJECT);
        Assert.assertEquals(3, sqls1.size());
        Assert.assertEquals("select CAL_DT from TEST_KYLIN_FACT", sqls1.get(0));
        Assert.assertEquals("select concat(';', LSTG_FORMAT_NAME), '123', 234, 'abc' from TEST_KYLIN_FACT",
                sqls1.get(1));
        Assert.assertEquals("select '456', 456, 'dgf' from TEST_KYLIN_FACT", sqls1.get(2));

        List<String> sqls2 = favoriteRuleService.transformFileToSqls(new MockMultipartFile("sqls6.sql", "sqls6.sql",
                "text/plain", new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls6.sql"))), PROJECT);
        Assert.assertEquals(1, sqls2.size());
        Assert.assertEquals("select concat(';', LSTG_FORMAT_NAME) from TEST_KYLIN_FACT", sqls2.get(0));

        List<String> sqls3 = favoriteRuleService.transformFileToSqls(new MockMultipartFile("sqls7.sql", "sqls7.sql",
                "text/plain", new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls7.sql"))), PROJECT);
        Assert.assertEquals(2, sqls3.size());
        Assert.assertEquals("select CAL_DT from TEST_KYLIN_FACT", sqls3.get(0));
        Assert.assertEquals("select concat(';', LSTG_FORMAT_NAME), '123', 234, 'abc' from TEST_KYLIN_FACT",
                sqls3.get(1));

        List<String> sqls4 = favoriteRuleService.transformFileToSqls(new MockMultipartFile("sqls8.sql", "sqls8.sql",
                "text/plain", new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls8.sql"))), PROJECT);
        Assert.assertEquals(360, sqls4.size());
    }

    @Test
    public void testSqlValidate() {
        String sql = "select * from test_kylin_fact\n\n";
        var response = favoriteRuleService.sqlValidate(PROJECT, sql);
        Assert.assertTrue(response.isCapable());
    }

    @Test
    public void testSqlValidateError() {
        String sql = "select * from test_kylin\n\n";
        var response = favoriteRuleService.sqlValidate(PROJECT, sql);
        Assert.assertFalse(response.isCapable());
        Assert.assertTrue(Lists.newArrayList(response.getSqlAdvices()).get(0).getIncapableReason()
                .contains("Can’t find table \"TEST_KYLIN\". Please check and try again."));
    }

    @Test
    public void testImportCCSQLs() {
        val ccDesc = new ComputedColumnDesc();
        ccDesc.setTableAlias("TEST_KYLIN_FACT");
        ccDesc.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        ccDesc.setColumnName("DEAL_AMOUNT");
        ccDesc.setDatatype("decimal(30,4)");
        ccDesc.setExpression("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT");

        val basicModel = NDataModelManager.getInstance(getTestConfig(), PROJECT)
                .getDataModelDescByAlias("nmodel_basic");
        Assert.assertTrue(basicModel.getComputedColumnDescs().contains(ccDesc));

        // PRICE * ITEM_COUNT expression already exists
        String sql = "SELECT SUM(PRICE * ITEM_COUNT), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";

        MockMultipartFile file1 = new MockMultipartFile("file.sql", "file.sql", "text/plain",
                sql.getBytes(StandardCharsets.UTF_8));
        SQLParserResponse result = favoriteRuleService.importSqls(new MultipartFile[] { file1 }, PROJECT);
        List<ImportSqlResponse> responses = result.getData();
        Assert.assertEquals(1, responses.size());
        Assert.assertTrue(responses.get(0).isCapable());
        Assert.assertEquals(1, result.getSize());
        Assert.assertEquals(1, result.getCapableSqlNum());

        // same cc expression not replaced with existed cc
        Assert.assertEquals(sql, responses.get(0).getSql());
    }

    @Test
    public void testImportSqlExceedsLimit() throws Exception {
        MockMultipartFile file1 = new MockMultipartFile("sqls1.sql", "sqls1.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls1.sql")));
        MockMultipartFile file2 = new MockMultipartFile("sqls2.txt", "sqls2.txt", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls2.txt")));
        MockMultipartFile file4 = new MockMultipartFile("sqls4.sql", "sqls4.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls4.sql")));

        var response = favoriteRuleService.importSqls(new MultipartFile[] { file4 }, PROJECT);
        Assert.assertEquals(1200, response.getSize());

        response = favoriteRuleService.importSqls(new MultipartFile[] { file1, file2, file4 }, PROJECT);
        Assert.assertEquals(1210, response.getSize());
    }

    @Test
    public void testLoadStreamingSqls() throws IOException {
        MockMultipartFile file1 = new MockMultipartFile("sqls9.sql", "sqls9.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls9.sql")));
        MockMultipartFile file2 = new MockMultipartFile("sqls10.sql", "sqls10.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls10.sql")));
        val response = favoriteRuleService.importSqls(new MultipartFile[] { file1, file2 }, "streaming_test");
        Assert.assertEquals(2, response.getSize());
        Assert.assertEquals(1, response.getCapableSqlNum());

    }
}
