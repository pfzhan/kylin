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

import java.util.Comparator;
import java.util.stream.Collectors;

import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.ModelSemanticUpdateEvent;
import lombok.val;

public class ModelSemanticUpdateHandlerTest extends NLocalFileMetadataTestCase {

    public static final String MODEL_NAME = "nmodel_basic";

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        getTestConfig().setProperty("kylin.server.mode", "query");
    }

    @After
    public void tearDown() throws Exception {
        getTestConfig().setProperty("kylin.server.mode", "all");
        cleanupTestMetadata();
    }

    @Test
    public void testChangeJoinType() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val originModel = getTestBasicModel();
        modelMgr.updateDataModel(MODEL_NAME, model -> {
            val joins = model.getJoinTables();
            joins.get(0).getJoin().setType("inner");
        });
        val updateEvent = new ModelSemanticUpdateEvent();
        updateEvent.setProject("default");
        updateEvent.setModelName(MODEL_NAME);
        updateEvent.setOriginModel(originModel);
        val eventContext = new EventContext(updateEvent, getTestConfig());
        val handler = new ModelSemanticUpdateHandler();
        handler.handle(eventContext);

        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Comparator.comparingLong(Event::getCreateTime));
        Assert.assertTrue(events.get(1) instanceof RefreshSegmentEvent);
    }

    @Test
    public void testOnlyAddDimensions() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val originModel = getTestBasicModel();
        modelMgr.updateDataModel(MODEL_NAME, model -> model.setAllNamedColumns(model.getAllNamedColumns().stream()
                .peek(c -> c.status = NDataModel.ColumnStatus.DIMENSION).collect(Collectors.toList())));
        val updateEvent = new ModelSemanticUpdateEvent();
        updateEvent.setProject("default");
        updateEvent.setModelName(MODEL_NAME);
        updateEvent.setOriginModel(originModel);
        val eventContext = new EventContext(updateEvent, getTestConfig());
        val handler = new ModelSemanticUpdateHandler();
        handler.handle(eventContext);

        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Comparator.comparingLong(Event::getCreateTime));
        Assert.assertEquals(1, events.size());
    }

    @Test
    public void testOnlyChangeMeasures() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), "default");
        val originModel = getTestBasicModel();
        modelMgr.updateDataModel(MODEL_NAME, model -> model.setAllMeasures(model.getAllMeasures().stream().peek(m -> {
            if (m.id == 1011) {
                m.id = 1012;
            }
        }).collect(Collectors.toList())));
        val updateEvent = new ModelSemanticUpdateEvent();
        updateEvent.setProject("default");
        updateEvent.setModelName(MODEL_NAME);
        updateEvent.setOriginModel(originModel);
        val eventContext = new EventContext(updateEvent, getTestConfig());
        val handler = new ModelSemanticUpdateHandler();
        handler.handle(eventContext);

        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Comparator.comparingLong(Event::getCreateTime));
        Assert.assertEquals(2, events.size());

        val cube = cubeMgr.getCubePlan("ncube_basic");
        for (NCuboidLayout layout : cube.getWhitelistCuboidLayouts()) {
            Assert.assertTrue(!layout.getColOrder().contains(1011));
            Assert.assertTrue(!layout.getCuboidDesc().getMeasures().contains(1011));
        }
    }

    @Test
    public void testOnlyChangeMeasuresWithRule() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), "default");
        val originModel = getTestInnerModel();
        modelMgr.updateDataModel(originModel.getName(),
                model -> model.setAllMeasures(model.getAllMeasures().stream().peek(m -> {
                    if (m.id == 1011) {
                        m.id = 1012;
                    }
                }).collect(Collectors.toList())));
        val updateEvent = new ModelSemanticUpdateEvent();
        updateEvent.setProject("default");
        updateEvent.setModelName(originModel.getName());
        updateEvent.setOriginModel(originModel);
        val eventContext = new EventContext(updateEvent, getTestConfig());
        val handler = new ModelSemanticUpdateHandler();
        handler.handle(eventContext);

        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Comparator.comparingLong(Event::getCreateTime));
        Assert.assertEquals(4, events.size());

        val cube = cubeMgr.getCubePlan("ncube_basic_inner");
        for (NCuboidLayout layout : cube.getWhitelistCuboidLayouts()) {
            Assert.assertTrue(!layout.getColOrder().contains(1011));
            Assert.assertTrue(!layout.getCuboidDesc().getMeasures().contains(1011));
        }
        val newRule = cube.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid();
        Assert.assertTrue(!newRule.getMeasures().contains(1011));
    }

    @Test
    public void testAllChanged() throws Exception {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), "default");
        val originModel = getTestInnerModel();
        modelMgr.updateDataModel(originModel.getName(),
                model -> model.setAllMeasures(model.getAllMeasures().stream().peek(m -> {
                    if (m.id == 1011) {
                        m.id = 1012;
                    }
                }).collect(Collectors.toList())));
        modelMgr.updateDataModel(originModel.getName(), model -> {
            val joins = model.getJoinTables();
            joins.get(0).getJoin().setType("inner");
        });
        modelMgr.updateDataModel(originModel.getName(), model -> model.setAllNamedColumns(model.getAllNamedColumns()
                .stream().peek(c -> {
                    c.status = NDataModel.ColumnStatus.DIMENSION;
                    if (c.id == 26) {
                        c.status = NDataModel.ColumnStatus.EXIST;
                    }
                }).collect(Collectors.toList())));
        val updateEvent = new ModelSemanticUpdateEvent();
        updateEvent.setProject("default");
        updateEvent.setModelName(originModel.getName());
        updateEvent.setOriginModel(originModel);
        val eventContext = new EventContext(updateEvent, getTestConfig());
        val handler = new ModelSemanticUpdateHandler();
        handler.handle(eventContext);

        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Comparator.comparingLong(Event::getCreateTime));
        Assert.assertEquals(4, events.size());

        val cube = cubeMgr.getCubePlan("ncube_basic_inner");
        for (NCuboidLayout layout : cube.getWhitelistCuboidLayouts()) {
            Assert.assertTrue(!layout.getColOrder().contains(1011));
            Assert.assertTrue(!layout.getColOrder().contains(26));
            Assert.assertTrue(!layout.getCuboidDesc().getMeasures().contains(1011));
            Assert.assertTrue(!layout.getCuboidDesc().getDimensions().contains(26));
        }
    }

    private NDataModel getTestInnerModel() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val model = modelMgr.getDataModelDesc("nmodel_basic_inner");
        return model;
    }

    private NDataModel getTestBasicModel() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "default");
        val model = modelMgr.getDataModelDesc("nmodel_basic");
        return model;
    }
}
