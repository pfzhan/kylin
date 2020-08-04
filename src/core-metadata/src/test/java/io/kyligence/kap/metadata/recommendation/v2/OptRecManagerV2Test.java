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

package io.kyligence.kap.metadata.recommendation.v2;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.util.RawRecStoreUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OptRecManagerV2Test extends NLocalFileMetadataTestCase {

    private static final String BASE_PATH = "../core-metadata/src/test/resources/rec_v2";
    private static final String MODEL_PATH_PATTERN = BASE_PATH + "/model_desc/%s.json";
    private static final String INDEX_PATH_PATTERN = BASE_PATH + "/index_plan/%s.json";
    private static final String REC_DIRECTORY = BASE_PATH + "/rec_items/";
    private static final String REC_PATH_PATTERN = BASE_PATH + "/rec_items/%s.json";
    private static final String H2_METADATA_URL_PATTERN = "%s@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=";

    private NDataModelManager modelManager;
    private NIndexPlanManager indexPlanManager;
    private JdbcRawRecStore jdbcRawRecStore;

    private String getProject() {
        return "ssb";
    }

    private String getDefaultUUID() {
        return "a4f5117e-a609-4750-8c04-a73fa7959227";
    }

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val managersByPrjCache = getInstanceByProjectFromSingleton();
        managersByPrjCache.put(RawRecManager.class, new ConcurrentHashMap<>());
        getTestConfig().setMetadataUrl(String.format(H2_METADATA_URL_PATTERN, "rec_opt"));
        List<RawRecItem> recItems = loadAllRecItems(REC_DIRECTORY);
        recItems.forEach(recItem -> recItem.setState(RawRecItem.RawRecState.INITIAL));
        jdbcRawRecStore = new JdbcRawRecStore(getTestConfig());
        recItems.sort(Comparator.comparingInt(RawRecItem::getId));
        jdbcRawRecStore.save(recItems);
    }

    private JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    @After
    public void tearDown() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
        this.cleanupTestMetadata();

        log.debug("clean SqlSessionFactory...");
        Class<RawRecStoreUtil> clazz = RawRecStoreUtil.class;
        Field sqlSessionFactory = clazz.getDeclaredField("sqlSessionFactory");
        sqlSessionFactory.setAccessible(true);
        sqlSessionFactory.set(null, null);
        log.debug("SqlSessionFactory was set to {}", sqlSessionFactory.get(null));
        sqlSessionFactory.setAccessible(false);
        log.debug("clean SqlSessionFactory success");
    }

    /**
     * ID = 3, Agg RawRecItem doesn't depend on ComputedColumn
     */
    @Test
    public void testInitRecommendationOfAggIndex() throws Exception {
        RawRecItem item = jdbcRawRecStore.queryById(3);
        List<RawRecItem> recommendedLayoutItems = Lists.newArrayList(item);
        changeLayoutRecItemState(recommendedLayoutItems, RawRecItem.RawRecState.RECOMMENDED);
        jdbcRawRecStore.update(recommendedLayoutItems);
        prepareModelAndIndex(getDefaultUUID());

        OptRecV2 optRecV2 = new OptRecV2(getProject(), getDefaultUUID());
        Map<Integer, RecommendationRef> columnRefs = optRecV2.getColumnRefs();
        Assert.assertEquals(27, columnRefs.size());
        columnRefs.forEach((refId, ref) -> {
            Assert.assertTrue(ref.getEntity() instanceof NDataModel.NamedColumn);
            Assert.assertTrue(ref.getId() >= 0);
            Assert.assertTrue(ref.isExisted());
            Assert.assertFalse(ref.isBroken());
            Assert.assertTrue(ref.getDependencies().isEmpty());
        });

        Map<Integer, RecommendationRef> dimensionRefs = optRecV2.getDimensionRefs();
        Assert.assertEquals(1, dimensionRefs.size());
        dimensionRefs.forEach((refId, ref) -> {
            Assert.assertEquals(-1, ref.getId());
            Assert.assertEquals("P_LINEORDER.LO_CUSTKEY", ref.getName());
            Assert.assertEquals("integer", ref.getDataType());
            Assert.assertFalse(ref.isExisted());
            Assert.assertFalse(ref.isBroken());
            Assert.assertEquals(1, ref.getDependencies().size());
            RecommendationRef recommendationRef = ref.getDependencies().get(0);
            Assert.assertTrue(recommendationRef instanceof ModelColumnRef);
            Assert.assertEquals(8, recommendationRef.getId());
        });

        Map<Integer, RecommendationRef> measureRefs = optRecV2.getMeasureRefs();
        Assert.assertEquals(2, measureRefs.size());
        measureRefs.forEach((refId, ref) -> {
            if (ref.getId() == 100000) {
                Assert.assertEquals("COUNT_ALL", ref.getName());
                Assert.assertTrue(ref.isExisted());
                Assert.assertFalse(ref.isBroken());
                Assert.assertTrue(ref.getDependencies().isEmpty());
            } else {
                Assert.assertEquals(-2, ref.getId());
                Assert.assertFalse(ref.isExisted());
                Assert.assertFalse(ref.isBroken());
                Assert.assertEquals(1, ref.getDependencies().size());
                RecommendationRef recommendationRef = ref.getDependencies().get(0);
                Assert.assertTrue(recommendationRef instanceof ModelColumnRef);
                Assert.assertEquals(3, recommendationRef.getId());
            }
        });

        Map<Integer, LayoutRef> layoutRefs = optRecV2.getLayoutRefs();
        Assert.assertEquals(1, layoutRefs.size());
        layoutRefs.forEach((refId, ref) -> {
            Assert.assertEquals(-3, ref.getId());
            Assert.assertTrue(ref.isAgg());
            Assert.assertFalse(ref.isExisted());
            Assert.assertFalse(ref.isBroken());
            List<RecommendationRef> dependencies = ref.getDependencies();
            Assert.assertEquals(3, dependencies.size());
            Assert.assertEquals(-1, dependencies.get(0).getId());
            Assert.assertEquals(100000, dependencies.get(1).getId());
            Assert.assertEquals(-2, dependencies.get(2).getId());
        });

        Map<Integer, RecommendationRef> ccRefs = optRecV2.getCcRefs();
        Assert.assertTrue(ccRefs.isEmpty());
    }

    /**
     * ID = 6, Agg RawRecItem depend on a ComputedColumn on model
     */
    @Test
    public void testInitRecommendationOfAggIndexWithCCOnModel() throws IOException {
        RawRecItem item = jdbcRawRecStore.queryById(6);
        List<RawRecItem> recommendedLayoutItems = Lists.newArrayList(item);
        changeLayoutRecItemState(recommendedLayoutItems, RawRecItem.RawRecState.RECOMMENDED);
        jdbcRawRecStore.update(recommendedLayoutItems);
        prepareModelAndIndex(getDefaultUUID());

        OptRecV2 optRecV2 = new OptRecV2(getProject(), getDefaultUUID());
        Map<Integer, RecommendationRef> columnRefs = optRecV2.getColumnRefs();
        Assert.assertEquals(27, columnRefs.size());
        columnRefs.forEach((refId, ref) -> {
            Assert.assertTrue(ref.getEntity() instanceof NDataModel.NamedColumn);
            Assert.assertTrue(ref.getId() >= 0);
            Assert.assertTrue(ref.isExisted());
            Assert.assertFalse(ref.isBroken());
            Assert.assertTrue(ref.getDependencies().isEmpty());
        });

        Map<Integer, RecommendationRef> dimensionRefs = optRecV2.getDimensionRefs();
        Assert.assertEquals(1, dimensionRefs.size());
        dimensionRefs.forEach((refId, ref) -> {
            Assert.assertEquals(-4, ref.getId());
            Assert.assertEquals("P_LINEORDER.TAXED_REVENUE", ref.getName());
            Assert.assertEquals("bigint", ref.getDataType());
            Assert.assertFalse(ref.isExisted());
            Assert.assertFalse(ref.isBroken());
            Assert.assertEquals(1, ref.getDependencies().size());
            RecommendationRef recommendationRef = ref.getDependencies().get(0);
            Assert.assertTrue(recommendationRef instanceof ModelColumnRef);
            Assert.assertEquals(26, recommendationRef.getId());
            Assert.assertEquals("P_LINEORDER.TAXED_REVENUE", recommendationRef.getName());
            Assert.assertEquals("P_LINEORDER.LO_TAX*P_LINEORDER.LO_REVENUE", recommendationRef.getContent());
            Assert.assertTrue(recommendationRef.isExisted());
        });
        Map<Integer, RecommendationRef> measureRefs = optRecV2.getMeasureRefs();
        Assert.assertEquals(2, measureRefs.size());
        measureRefs.forEach((refId, ref) -> {
            if (ref.getId() == 100000) {
                return;
            }
            Assert.assertEquals(-5, ref.getId());
            Assert.assertEquals(1, ref.getDependencies().size());
            RecommendationRef dependRef = ref.getDependencies().get(0);
            Assert.assertEquals(5, dependRef.getId());

        });
        Map<Integer, RecommendationRef> ccRefs = optRecV2.getCcRefs();
        Map<Integer, LayoutRef> layoutRefs = optRecV2.getLayoutRefs();
    }

    @Test
    public void testInitRecommendationOfAggIndexWithProposedCC() throws IOException {
        RawRecItem item = jdbcRawRecStore.queryById(6);
        List<RawRecItem> recommendedLayoutItems = Lists.newArrayList(item);
        changeLayoutRecItemState(recommendedLayoutItems, RawRecItem.RawRecState.RECOMMENDED);
        jdbcRawRecStore.update(recommendedLayoutItems);
        prepareModelAndIndex(getDefaultUUID());

        OptRecV2 optRecV2 = new OptRecV2(getProject(), getDefaultUUID());
        Map<Integer, RecommendationRef> columnRefs = optRecV2.getColumnRefs();
        Map<Integer, RecommendationRef> dimensionRefs = optRecV2.getDimensionRefs();
        Map<Integer, RecommendationRef> measureRefs = optRecV2.getMeasureRefs();
        Map<Integer, RecommendationRef> ccRefs = optRecV2.getCcRefs();
        Map<Integer, LayoutRef> layoutRefs = optRecV2.getLayoutRefs();
    }

    @Test
    public void testInitRecommendationOfTableIndex() throws IOException {
        RawRecItem item = jdbcRawRecStore.queryById(6);
        List<RawRecItem> recommendedLayoutItems = Lists.newArrayList(item);
        changeLayoutRecItemState(recommendedLayoutItems, RawRecItem.RawRecState.RECOMMENDED);
        jdbcRawRecStore.update(recommendedLayoutItems);
        prepareModelAndIndex(getDefaultUUID());

        OptRecV2 optRecV2 = new OptRecV2(getProject(), getDefaultUUID());
        Map<Integer, RecommendationRef> columnRefs = optRecV2.getColumnRefs();
        Map<Integer, RecommendationRef> dimensionRefs = optRecV2.getDimensionRefs();
        Map<Integer, RecommendationRef> measureRefs = optRecV2.getMeasureRefs();
        Map<Integer, RecommendationRef> ccRefs = optRecV2.getCcRefs();
        Map<Integer, LayoutRef> layoutRefs = optRecV2.getLayoutRefs();

    }

    @Test
    public void testInitRecommendationOfTableIndexWithCCOnModel() throws IOException {
        RawRecItem item = jdbcRawRecStore.queryById(6);
        List<RawRecItem> recommendedLayoutItems = Lists.newArrayList(item);
        changeLayoutRecItemState(recommendedLayoutItems, RawRecItem.RawRecState.RECOMMENDED);
        jdbcRawRecStore.update(recommendedLayoutItems);
        prepareModelAndIndex(getDefaultUUID());

        OptRecV2 optRecV2 = new OptRecV2(getProject(), getDefaultUUID());
        Map<Integer, RecommendationRef> columnRefs = optRecV2.getColumnRefs();
        Map<Integer, RecommendationRef> dimensionRefs = optRecV2.getDimensionRefs();
        Map<Integer, RecommendationRef> measureRefs = optRecV2.getMeasureRefs();
        Map<Integer, RecommendationRef> ccRefs = optRecV2.getCcRefs();
        Map<Integer, LayoutRef> layoutRefs = optRecV2.getLayoutRefs();

    }

    @Test
    public void testInitErrorForCCOnModelMissing() throws Exception {
        RawRecItem item = jdbcRawRecStore.queryById(6);
        List<RawRecItem> recommendedLayoutItems = Lists.newArrayList(item);
        changeLayoutRecItemState(recommendedLayoutItems, RawRecItem.RawRecState.RECOMMENDED);
        jdbcRawRecStore.update(recommendedLayoutItems);
        prepareModelAndIndex(getDefaultUUID());

        OptRecV2 optRecV2 = new OptRecV2(getProject(), getDefaultUUID());
        Map<Integer, RecommendationRef> columnRefs = optRecV2.getColumnRefs();
        Map<Integer, RecommendationRef> dimensionRefs = optRecV2.getDimensionRefs();
        Map<Integer, RecommendationRef> measureRefs = optRecV2.getMeasureRefs();
        Map<Integer, RecommendationRef> ccRefs = optRecV2.getCcRefs();
        Map<Integer, LayoutRef> layoutRefs = optRecV2.getLayoutRefs();

    }

    @Test
    public void testInitErrorForColumnOnModelMissing() throws IOException {
        RawRecItem item = jdbcRawRecStore.queryById(6);
        List<RawRecItem> recommendedLayoutItems = Lists.newArrayList(item);
        changeLayoutRecItemState(recommendedLayoutItems, RawRecItem.RawRecState.RECOMMENDED);
        jdbcRawRecStore.update(recommendedLayoutItems);
        prepareModelAndIndex(getDefaultUUID());

        OptRecV2 optRecV2 = new OptRecV2(getProject(), getDefaultUUID());
        Map<Integer, RecommendationRef> columnRefs = optRecV2.getColumnRefs();
        Map<Integer, RecommendationRef> dimensionRefs = optRecV2.getDimensionRefs();
        Map<Integer, RecommendationRef> measureRefs = optRecV2.getMeasureRefs();
        Map<Integer, RecommendationRef> ccRefs = optRecV2.getCcRefs();
        Map<Integer, LayoutRef> layoutRefs = optRecV2.getLayoutRefs();
    }

    @Test
    public void testInitErrorForMeasureOnModelMissing() throws IOException {
        RawRecItem item = jdbcRawRecStore.queryById(6);
        List<RawRecItem> recommendedLayoutItems = Lists.newArrayList(item);
        changeLayoutRecItemState(recommendedLayoutItems, RawRecItem.RawRecState.RECOMMENDED);
        jdbcRawRecStore.update(recommendedLayoutItems);
        prepareModelAndIndex(getDefaultUUID());

        OptRecV2 optRecV2 = new OptRecV2(getProject(), getDefaultUUID());
        Map<Integer, RecommendationRef> columnRefs = optRecV2.getColumnRefs();
        Map<Integer, RecommendationRef> dimensionRefs = optRecV2.getDimensionRefs();
        Map<Integer, RecommendationRef> measureRefs = optRecV2.getMeasureRefs();
        Map<Integer, RecommendationRef> ccRefs = optRecV2.getCcRefs();
        Map<Integer, LayoutRef> layoutRefs = optRecV2.getLayoutRefs();
    }

    private void prepareModelAndIndex(String uuid) throws IOException {
        NDataModel dataModel = JsonUtil.readValue(new File(String.format(MODEL_PATH_PATTERN, uuid)), NDataModel.class);
        IndexPlan indexPlan = JsonUtil.readValue(new File(String.format(INDEX_PATH_PATTERN, uuid)), IndexPlan.class);
        modelManager.createDataModelDesc(dataModel, dataModel.getOwner());
        indexPlanManager.createIndexPlan(indexPlan);
    }

    private void changeLayoutRecItemState(List<RawRecItem> allRecItems, RawRecItem.RawRecState state) {
        allRecItems.forEach(recItem -> recItem.setState(state));
    }

    private List<RawRecItem> loadAllRecItems(String dirPath) throws IOException {
        List<RawRecItem> allRecItems = Lists.newArrayList();
        File directory = new File(dirPath);
        File[] files = directory.listFiles();
        for (File file : files) {
            String uuid = file.getName().substring(0, file.getName().lastIndexOf('.'));
            String recItemContent = FileUtils.readFileToString(new File(String.format(REC_PATH_PATTERN, uuid)));
            allRecItems.addAll(parseRecItems(recItemContent));
        }
        return allRecItems;
    }

    private List<RawRecItem> parseRecItems(String recItemContent) throws IOException {
        List<RawRecItem> recItems = Lists.newArrayList();
        JsonNode jsonNode = JsonUtil.readValueAsTree(recItemContent);
        final Iterator<JsonNode> elements = jsonNode.elements();
        while (elements.hasNext()) {
            JsonNode recItemNode = elements.next();
            RawRecItem item = parseRawRecItem(recItemNode);
            recItems.add(item);
        }
        return recItems;
    }

    private RawRecItem parseRawRecItem(JsonNode recItemNode) {
        RawRecItem item = new RawRecItem();
        item.setId(recItemNode.get("id").asInt());
        item.setProject(recItemNode.get("project").asText());
        item.setModelID(recItemNode.get("model_id").asText());
        item.setUniqueFlag(recItemNode.get("unique_flag").asText());
        item.setSemanticVersion(recItemNode.get("semantic_version").asInt());
        byte type = (byte) recItemNode.get("type").asInt();
        item.setType(RawRecItem.toRecType(type));
        item.setRecEntity(RawRecItem.toRecItem(recItemNode.get("rec_entity").asText(), type));
        item.setDependIDs(RawRecItem.toDependIds(recItemNode.get("depend_ids").asText()));
        // item.setLayoutMetric(null)
        item.setCost(recItemNode.get("cost").asDouble());
        item.setTotalLatencyOfLastDay(recItemNode.get("total_latency_of_last_day").asDouble());
        item.setHitCount(recItemNode.get("hit_count").asInt());
        item.setTotalTime(recItemNode.get("total_time").asDouble());
        item.setMaxTime(recItemNode.get("max_time").asDouble());
        item.setMinTime(recItemNode.get("min_time").asDouble());
        item.setState(RawRecItem.toRecState((byte) recItemNode.get("state").asInt()));
        item.setUpdateTime(recItemNode.get("update_time").asLong());
        item.setCreateTime(recItemNode.get("create_time").asLong());
        return item;
    }

}
