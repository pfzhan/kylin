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
package io.kyligence.kap.event.handle;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.event.model.AccelerateEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryDao;
import io.kyligence.kap.metadata.favorite.FavoriteQueryJDBCDao;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

@Slf4j
public class AccelerateEventHandlerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        getTestConfig().setProperty("kylin.favorite.storage-url",
                "kylin_favorite@jdbc,url=jdbc:h2:mem:db_default;MODE=MySQL,username=sa,password=,driverClassName=org.h2.Driver");
    }

    @After
    public void tearDown() throws Exception {
        FavoriteQueryJDBCDao favoriteQueryJDBCDao = FavoriteQueryJDBCDao.getInstance(getTestConfig());
        favoriteQueryJDBCDao.dropTable();
        this.cleanupTestMetadata();
    }

    @Test
    public void testHandlerIdempotent() throws Exception {
        String sqlPattern = "select CAL_DT, LSTG_FORMAT_NAME, sum(PRICE), sum(ITEM_COUNT) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME";
        loadTestDataToDao(sqlPattern);

        getTestConfig().setProperty("kylin.server.mode", "query");

        AccelerateEvent event = new AccelerateEvent();
        event.setModels(Lists.newArrayList());
        event.setFavoriteMark(true);
        event.setProject(DEFAULT_PROJECT);
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NCubePlan cubePlan1 = cubePlanManager.getCubePlan("all_fixed_length");
        logLayouts(cubePlan1);
        int layoutCount1 = cubePlan1.getAllCuboidLayouts().size();

        event.setSqlPatterns(Lists.newArrayList(sqlPattern));
        EventContext eventContext = new EventContext(event, getTestConfig());
        AccelerateEventHandler handler = new AccelerateEventHandler();
        // add favorite sql to update model and post an new AddCuboidEvent
        handler.handle(eventContext);

        NCubePlan cubePlan2 = cubePlanManager.getCubePlan("all_fixed_length");
        logLayouts(cubePlan2);
        int layoutCount2 = cubePlan2.getAllCuboidLayouts().size();
        Assert.assertEquals(layoutCount1 + 1, layoutCount2);

        List<Event> events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
        Assert.assertNotNull(events);
        Assert.assertEquals(2, events.size());

        // the status of corresponding favorite sql changed to accelerating
        FavoriteQueryDao favoriteQueryDao = FavoriteQueryJDBCDao.getInstance(getTestConfig());
        FavoriteQuery favoriteQuery = ((FavoriteQueryJDBCDao) favoriteQueryDao).getFavoriteQuery(sqlPattern.hashCode(), DEFAULT_PROJECT);
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, favoriteQuery.getStatus());

        // run again, and model will not update and will not post an new AddCuboidEvent
        handler.handle(eventContext);

        NCubePlan cubePlan3 = cubePlanManager.getCubePlan("all_fixed_length");
        int layoutCount3 = cubePlan3.getAllCuboidLayouts().size();
        Assert.assertEquals(layoutCount3, layoutCount2);

        // now the status changed to fully accelerated
        favoriteQuery = ((FavoriteQueryJDBCDao) favoriteQueryDao).getFavoriteQuery(sqlPattern.hashCode(), DEFAULT_PROJECT);
        Assert.assertEquals(FavoriteQueryStatusEnum.FULLY_ACCELERATED, favoriteQuery.getStatus());

        events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
        Assert.assertNotNull(events);
        Assert.assertEquals(2, events.size());

        getTestConfig().setProperty("kylin.server.mode", "all");

    }

    private void logLayouts(NCubePlan cubePlan) {
        for (NCuboidLayout layout : cubePlan.getAllCuboidLayouts()) {
            log.debug("layout id:{} -- {}, auto:{}, manual:{}, col:{}, sort:{}", layout.getId(),
                    layout.getCuboidDesc().getId(), layout.isAuto(), layout.isManual(), layout.getColOrder(),
                    layout.getSortByColumns());
        }
    }

    @Test
    public void testAccelerateSqlIsBlocked() throws Exception {
        getTestConfig().setProperty("kylin.server.mode", "query");
        getTestConfig().setProperty("kylin.favorite.storage-url", "kylin_favorite@jdbc,url=jdbc:h2:mem:db_default;MODE=MySQL,username=sa,password=,driverClassName=org.h2.Driver");

        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = projectManager.getProject(DEFAULT_PROJECT);
        projectInstance = projectManager.copyForWrite(projectInstance);
        projectInstance.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectInstance);

        NDataModel dataModel = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getDataModelDesc("all_fixed_length");

        for (NDataModel.NamedColumn namedColumn : dataModel.getAllNamedColumns()) {
            if (namedColumn.getId() == 16) {
                Class<?> clazz = namedColumn.getClass();
                Field field = clazz.getDeclaredField("status");
                field.setAccessible(true);
                field.set(namedColumn, NDataModel.ColumnStatus.EXIST);
            }
        }

        NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT).updateDataModelDesc(dataModel.copy());


        String sql = "select CAL_DT, LSTG_FORMAT_NAME, max(SLR_SEGMENT_CD) " +
                "from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME";
        FavoriteQuery favoriteQuery = new FavoriteQuery(sql, sql.hashCode(), DEFAULT_PROJECT, System.currentTimeMillis() + 3, 5, 100);
        FavoriteQueryJDBCDao.getInstance(getTestConfig()).batchInsert(Lists.newArrayList(favoriteQuery));

        AccelerateEvent event = new AccelerateEvent();
        event.setFavoriteMark(true);
        event.setModels(Lists.newArrayList());
        event.setProject(DEFAULT_PROJECT);
        event.setSqlPatterns(Lists.newArrayList(sql));
        EventContext eventContext = new EventContext(event, getTestConfig());
        AccelerateEventHandler handler = new AccelerateEventHandler();

        handler.handle(eventContext);

        List<Event> events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
        // the sql is blocked, so that there is no AddCuboidEvent, there is only the AccelerateEvent
        Assert.assertEquals(1, events.size());
        Assert.assertEquals(AccelerateEvent.class, events.get(0).getClass());

        // the query's status is updated to blocked
        favoriteQuery = FavoriteQueryJDBCDao.getInstance(getTestConfig()).getFavoriteQuery(sql.hashCode(), DEFAULT_PROJECT);
        Assert.assertEquals(FavoriteQueryStatusEnum.BLOCKED, favoriteQuery.getStatus());

        projectInstance = projectManager.getProject(DEFAULT_PROJECT);
        projectInstance = projectManager.copyForWrite(projectInstance);
        projectInstance.setMaintainModelType(MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(projectInstance);
        getTestConfig().setProperty("kylin.server.mode", "all");
    }


    private void loadTestDataToDao(String sqlPattern) {
        FavoriteQueryDao favoriteQueryDao = FavoriteQueryJDBCDao.getInstance(getTestConfig());
        FavoriteQuery favoriteQuery = new FavoriteQuery(sqlPattern, sqlPattern.hashCode(), DEFAULT_PROJECT);
        favoriteQueryDao.batchInsert(Lists.newArrayList(favoriteQuery));
    }
}
