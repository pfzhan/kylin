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
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.rest.request.ModelImportRequest;
import lombok.val;

public class MetaStoreServiceWithRecTest extends ServiceTestBase {
    @Autowired
    private MetaStoreService metaStoreService;

    @Autowired
    private ModelService modelService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    JdbcTemplate jdbcTemplate = null;
    JdbcRawRecStore jdbcRawRecStore = null;
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        createTestMetadata("src/test/resources/ut_meta/metastore_model");
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        try {
            SecurityContextHolder.getContext().setAuthentication(authentication);
            jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
        } catch (Exception e) {
            e.printStackTrace();
        }
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        try {
            jdbcRawRecStore = new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        cleanupTestMetadata();
    }

    @Test
    public void testImportModelMetadataWithRec() throws Exception {
        File file = new File(
                "src/test/resources/ut_model_metadata/target_project_model_metadata_2020_11_21_16_40_43_61D23206229CEB0C078F24AAACADF5DB.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        models.add(new ModelImportRequest.ModelImport("model_index", "model_index",
                ModelImportRequest.ImportType.OVERWRITE));

        request.setModels(models);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "original_project");
        NDataModel dataModel = dataModelManager.getDataModelDescByAlias("model_index");
        List<RawRecItem> rawRecItems = jdbcRawRecStore.listAll("original_project", dataModel.getUuid(), 1, 10);
        Assert.assertEquals(0, rawRecItems.size());

        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = projectManager.getProject("original_project");

        Assert.assertTrue(projectInstance.isExpertMode());

        projectManager.updateProject("original_project", copyForWrite -> {
            copyForWrite.putOverrideKylinProps("kylin.metadata.semi-automatic-mode", String.valueOf(true));
        });

        getTestConfig().clearManagers();
        projectManager = NProjectManager.getInstance(getTestConfig());
        projectInstance = projectManager.getProject("original_project");

        Assert.assertFalse(projectInstance.isExpertMode());
        metaStoreService.importModelMetadata("original_project", multipartFile, request);
        dataModel = dataModelManager.getDataModelDescByAlias("model_index");

        rawRecItems = jdbcRawRecStore.listAll("original_project", dataModel.getUuid(), dataModel.getSemanticVersion(),
                10);
        Assert.assertEquals(3, rawRecItems.size());

        val nDataModelManager = NDataModelManager.getInstance(getTestConfig(), "original_project");
        val nDataModel = nDataModelManager.getDataModelDescByAlias("model_index");
        Assert.assertEquals(2, nDataModel.getRecommendationsCount());
    }

}
