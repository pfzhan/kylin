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

package io.kyligence.kap.metadata.favorite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class FavoriteQueryRealizationJDBCDaoTest extends NLocalFileMetadataTestCase{

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testDaoInsertAndDelete(){
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.favorite.storage-url", "kylin_favorite@jdbc,url=jdbc:h2:mem:db_default;MODE=MySQL,username=sa,password=,driverClassName=org.h2.Driver");
        FavoriteQueryRealizationJDBCDao dao = FavoriteQueryRealizationJDBCDao.getInstance(config, "default");
        List<FavoriteQueryRealization> favoriteQueryRealizations = new ArrayList<>();
        FavoriteQueryRealization realization = new FavoriteQueryRealization();
        favoriteQueryRealizations.add(realization);
        String sql = "select * from demo";
        String modelId = "test";
        String cubePlanId = "test";

        realization.setSqlPatternHash(sql.hashCode());
        realization.setModelId(modelId);
        realization.setCubePlanId(cubePlanId);
        realization.setCuboidLayoutId(10000L);

        // test insert
        dao.batchInsert(favoriteQueryRealizations);
        List<FavoriteQueryRealization> list = dao.getByConditions(modelId, cubePlanId, 10000L);
        Assert.assertEquals(list.size(), 1);

        list = dao.getBySqlPatternHash(sql.hashCode());
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getSqlPatternHash(), sql.hashCode());

        // test delete
        dao.batchDelete(favoriteQueryRealizations);
        list = dao.getByConditions(modelId, cubePlanId, 10000L);
        Assert.assertEquals(0, list.size());

    }

    @Test
    public void testInsertOrUpdateExceptionAndRollback(){
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String errorMsg = "catch me";
        config.setProperty("kylin.favorite.storage-url", "kylin_favorite@jdbc,url=jdbc:h2:mem:db_default;MODE=MySQL,username=sa,password=,driverClassName=org.h2.Driver");
        FavoriteQueryRealizationJDBCDao dao = FavoriteQueryRealizationJDBCDao.getInstance(config, "default");
        List<FavoriteQueryRealization> favoriteQueryRealizations = new ArrayList<>();
        FavoriteQueryRealization realization = new FavoriteQueryRealization();
        favoriteQueryRealizations.add(realization);
        String sql = "select * from exception";
        String modelId = "test_error";
        String cubePlanId = "test";
        realization.setSqlPatternHash(sql.hashCode());
        realization.setModelId(modelId);
        realization.setCubePlanId(cubePlanId);
        realization.setCuboidLayoutId(30000L);

        // a record throw RuntimeException to test transaction and roll back
        FavoriteQueryRealization realizationError = Mockito.spy(FavoriteQueryRealization.class);
        Mockito.doThrow(new RuntimeException(errorMsg)).when(realizationError).getSqlPatternHash();
        favoriteQueryRealizations.add(realizationError);

        List<FavoriteQueryRealization> list = dao.getByConditions(modelId, null, null);
        int size = list.size();

        try {
            dao.batchInsert(favoriteQueryRealizations);
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertEquals(errorMsg, e.getMessage());
        }

        list = dao.getByConditions(modelId, null, null);
        // no record insert into db
        Assert.assertEquals(size, list.size());
    }

    @Test
    public void testBatchDeleteExceptionAndRollback(){
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String errorMsg = "catch me";
        config.setProperty("kylin.favorite.storage-url", "kylin_favorite@jdbc,url=jdbc:h2:mem:db_default;MODE=MySQL,username=sa,password=,driverClassName=org.h2.Driver");
        FavoriteQueryRealizationJDBCDao dao = FavoriteQueryRealizationJDBCDao.getInstance(config, "default");
        List<FavoriteQueryRealization> favoriteQueryRealizations = new ArrayList<>();
        FavoriteQueryRealization realization = new FavoriteQueryRealization();
        favoriteQueryRealizations.add(realization);
        String sql = "select * from exception2";
        String modelId = "test_delete";
        String cubePlanId = "test";
        realization.setSqlPatternHash(sql.hashCode());
        realization.setModelId(modelId);
        realization.setCubePlanId(cubePlanId);
        realization.setCuboidLayoutId(30000L);

        realization = new FavoriteQueryRealization();
        favoriteQueryRealizations.add(realization);
        sql = "select * from exception3";
        realization.setSqlPatternHash(sql.hashCode());
        realization.setModelId(modelId);
        realization.setCubePlanId(cubePlanId);
        realization.setCuboidLayoutId(30000L);

        dao.batchInsert(favoriteQueryRealizations);

        // a record throw RuntimeException to test transaction and roll back
        FavoriteQueryRealization realizationError = Mockito.spy(FavoriteQueryRealization.class);
        Mockito.doThrow(new RuntimeException(errorMsg)).when(realizationError).getSqlPatternHash();
        favoriteQueryRealizations.add(realizationError);

        List<FavoriteQueryRealization> list = dao.getByConditions(modelId, null, null);
        int size = list.size();

        try {
            dao.batchDelete(favoriteQueryRealizations);
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertEquals(errorMsg, e.getMessage());
        }

        list = dao.getByConditions(modelId, null, null);
        // no record insert into db
        Assert.assertEquals(size, list.size());
    }

    @Test
    public void testTooLongProjectName() throws IOException {
        val projectManager = NProjectManager.getInstance(getTestConfig());
        val project = projectManager.copyForWrite(projectManager.getProject("default"));
        String projectName = RandomStringUtils.random(200, true, true);
        projectManager.createProject(projectName, project.getOwner(), project.getDescription(),
                project.getOverrideKylinProps(), project.getMaintainModelType());

        FavoriteQueryRealizationJDBCDao favoriteQueryRealizationJDBCDao = FavoriteQueryRealizationJDBCDao.getInstance(getTestConfig(), projectName);
        Assert.assertNotNull(favoriteQueryRealizationJDBCDao);
    }

}
