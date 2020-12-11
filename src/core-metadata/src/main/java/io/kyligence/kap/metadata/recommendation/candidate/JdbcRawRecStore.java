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

package io.kyligence.kap.metadata.recommendation.candidate;

import static org.apache.commons.lang3.time.DateUtils.MILLIS_PER_DAY;
import static org.mybatis.dynamic.sql.SqlBuilder.count;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isIn;
import static org.mybatis.dynamic.sql.SqlBuilder.isLessThan;
import static org.mybatis.dynamic.sql.SqlBuilder.isNotEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isNotIn;
import static org.mybatis.dynamic.sql.SqlBuilder.max;
import static org.mybatis.dynamic.sql.SqlBuilder.min;
import static org.mybatis.dynamic.sql.SqlBuilder.select;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.render.UpdateStatementProvider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.util.RawRecStoreUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcRawRecStore {

    public static final String RECOMMENDATION_CANDIDATE = "_rec_candidate";
    private static final int NON_EXIST_MODEL_SEMANTIC_VERSION = Integer.MIN_VALUE;

    private final RawRecItemTable table;
    @VisibleForTesting
    @Getter
    private final SqlSessionFactory sqlSessionFactory;

    public JdbcRawRecStore(KylinConfig config) throws Exception {
        StorageURL url = config.getMetadataUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        DataSource dataSource = JdbcDataSource.getDataSource(props);
        table = new RawRecItemTable(genRawRecTableName(url));
        sqlSessionFactory = RawRecStoreUtil.getSqlSessionFactory(dataSource, table.tableNameAtRuntime());
    }

    private String genRawRecTableName(StorageURL url) {
        String tablePrefix = KylinConfig.getInstanceFromEnv().isUTEnv() ? "test_opt" : url.getIdentifier();
        return tablePrefix + RECOMMENDATION_CANDIDATE;
    }

    public void save(RawRecItem recItem) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            InsertStatementProvider<RawRecItem> insertStatement = getInsertProvider(recItem);
            int rows = mapper.insert(insertStatement);
            session.commit();
            if (rows > 0) {
                log.debug("Insert one raw recommendation({}) into database.", recItem.getUniqueFlag());
            } else {
                log.debug("No raw recommendation has been inserted into database.");
            }
        }
    }

    public void save(List<RawRecItem> recItems) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            List<InsertStatementProvider<RawRecItem>> providers = Lists.newArrayList();
            recItems.forEach(recItem -> providers.add(getInsertProvider(recItem)));
            providers.forEach(mapper::insert);
            session.commit();
            log.info("Insert {} raw recommendations into database takes {} ms", recItems.size(),
                    System.currentTimeMillis() - startTime);
        }
    }

    public RawRecItem queryById(int id) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = getSelectByIdStatementProvider(id);
            return mapper.selectOne(statementProvider);
        }
    }

    public RawRecItem queryByUniqueFlag(String project, String modelId, String uniqueFlag, Integer semanticVersion) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = getSelectUniqueFlagIdStatementProvider(project, modelId,
                    uniqueFlag, semanticVersion);
            return mapper.selectOne(statementProvider);
        }
    }

    public List<RawRecItem> queryAll() {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<RawRecItem> listAll(String project, String model, int semanticVersion, int limit) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .limit(limit) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> rawRecItems = mapper.selectMany(statementProvider);
            log.info("List all raw recommendations of model({}/{}, semanticVersion: {}) takes {} ms.", //
                    project, model, semanticVersion, System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    public List<RawRecItem> queryAdditionalLayoutRecItems(String project, String model) {
        int semanticVersion = getSemanticVersion(project, model);
        if (semanticVersion == NON_EXIST_MODEL_SEMANTIC_VERSION) {
            log.debug("model({}/{}) does not exist.", project, model);
            return Lists.newArrayList();
        }
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(RawRecItem.RawRecType.ADDITIONAL_LAYOUT)) //
                    .and(table.state, isEqualTo(RawRecItem.RawRecState.RECOMMENDED)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> rawRecItems = mapper.selectMany(statementProvider);
            log.info("Query raw recommendations can add indexes to model({}/{}, semanticVersion: {}) takes {} ms.", //
                    project, model, semanticVersion, System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    public List<RawRecItem> chooseTopNCandidates(String project, String model, int topN, RawRecItem.RawRecState state) {
        int semanticVersion = getSemanticVersion(project, model);
        if (semanticVersion == NON_EXIST_MODEL_SEMANTIC_VERSION) {
            log.debug("model({}/{}) does not exist.", project, model);
            return Lists.newArrayList();
        }
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(RawRecItem.RawRecType.ADDITIONAL_LAYOUT)) //
                    .and(table.state, isEqualTo(state)) //
                    .and(table.recSource, isNotEqualTo(RawRecItem.IMPORTED)) //
                    .orderBy(table.cost.descending(), table.hitCount.descending(), table.id.descending()) //
                    .limit(topN) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> rawRecItems = mapper.selectMany(statementProvider);
            log.info("Query topN({}) recommendations for adding to model({}/{}, semanticVersion: {}) takes {} ms.", //
                    topN, project, model, semanticVersion, System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    public List<RawRecItem> queryImportedRawRecItems(String project, String model, RawRecItem.RawRecState state) {
        int semanticVersion = getSemanticVersion(project, model);
        if (semanticVersion == NON_EXIST_MODEL_SEMANTIC_VERSION) {
            log.debug("model({}/{}) does not exist.", project, model);
            return Lists.newArrayList();
        }
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(RawRecItem.RawRecType.ADDITIONAL_LAYOUT)) //
                    .and(table.state, isEqualTo(state)) //
                    .and(table.recSource, isEqualTo(RawRecItem.IMPORTED)) //
                    .orderBy(table.id.descending()) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> rawRecItems = mapper.selectMany(statementProvider);
            log.info("Query recommendations "
                    + "generated from imported sql for adding to model({}/{}, semanticVersion: {}) takes {} ms.", //
                    project, model, semanticVersion, System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    public List<RawRecItem> queryNonAppliedLayoutRecItems(String project, String model, boolean isAdditionalRec) {
        int semanticVersion = getSemanticVersion(project, model);
        if (semanticVersion == NON_EXIST_MODEL_SEMANTIC_VERSION) {
            log.debug("model({}/{}) does not exist.", project, model);
            return Lists.newArrayList();
        }
        RawRecItem.RawRecType type = isAdditionalRec //
                ? RawRecItem.RawRecType.ADDITIONAL_LAYOUT //
                : RawRecItem.RawRecType.REMOVAL_LAYOUT;
        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(type)) //
                    .and(table.state, isNotEqualTo(RawRecItem.RawRecState.APPLIED)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> recItems = mapper.selectMany(statementProvider);
            log.info("Query raw recommendations of model({}/{}, semanticVersion: {}, type: {}) takes {} ms", //
                    project, model, semanticVersion, type.name(), System.currentTimeMillis() - start);
            return recItems;
        }
    }

    public List<RawRecItem> queryNonLayoutRecItems(String project, String model) {
        int semanticVersion = getSemanticVersion(project, model);
        if (semanticVersion == NON_EXIST_MODEL_SEMANTIC_VERSION) {
            log.debug("model({}/{}) does not exist.", project, model);
            return Lists.newArrayList();
        }
        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type,
                            isNotIn(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, RawRecItem.RawRecType.REMOVAL_LAYOUT)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> recItems = mapper.selectMany(statementProvider);
            log.info("Query non-index raw recommendations of model({}/{}, semanticVersion: {}) takes {} ms", //
                    project, model, semanticVersion, System.currentTimeMillis() - start);
            return recItems;
        }
    }

    private int getSemanticVersion(String project, String model) {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel dataModel = modelManager.getDataModelDesc(model);
        if (dataModel == null || dataModel.isBroken()) {
            return NON_EXIST_MODEL_SEMANTIC_VERSION;
        }
        return dataModel.getSemanticVersion();
    }

    public void updateState(List<Integer> idList, RawRecItem.RawRecState state) {
        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            List<UpdateStatementProvider> providers = Lists.newArrayList();
            long updateTime = System.currentTimeMillis();
            idList.forEach(id -> providers.add(changeRecStateProvider(id, state, updateTime)));
            providers.forEach(mapper::update);
            session.commit();
            log.info("Update {} raw recommendation(s) to state({}) takes {} ms", idList.size(), state.name(),
                    System.currentTimeMillis() - start);
        }
    }

    public void update(RawRecItem recItem) {
        // no need to update type and create_time
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            UpdateStatementProvider updateStatement = getUpdateProvider(recItem);
            mapper.update(updateStatement);
            session.commit();
            log.debug("Update one raw recommendation({})", recItem.getUniqueFlag());
        }
    }

    public void update(List<RawRecItem> recItems) {
        if (recItems == null || recItems.isEmpty()) {
            log.info("No raw recommendations need to update.");
            return;
        }

        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            List<UpdateStatementProvider> providers = Lists.newArrayList();
            recItems.forEach(item -> providers.add(getUpdateProvider(item)));
            providers.forEach(mapper::update);
            session.commit();
            log.info("Update {} raw recommendation(s) takes {} ms", recItems.size(),
                    System.currentTimeMillis() - start);
        }
    }

    public void batchAddOrUpdate(List<RawRecItem> recItems) {
        if (recItems == null || recItems.isEmpty()) {
            log.info("No raw recommendations need to add or update.");
            return;
        }

        List<RawRecItem> recItemsToAdd = Lists.newArrayList();
        List<RawRecItem> recItemsToUpdate = Lists.newArrayList();
        recItems.forEach(recItem -> {
            if (recItem.getId() == 0) {
                recItemsToAdd.add(recItem);
            } else {
                recItemsToUpdate.add(recItem);
            }
        });
        if (!recItemsToAdd.isEmpty()) {
            save(recItemsToAdd);
        }
        if (!recItemsToUpdate.isEmpty()) {
            update(recItemsToUpdate);
        }
    }

    public void batchUpdate(List<RawRecItem> recItems) {
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            List<UpdateStatementProvider> updaters = Lists.newArrayList();
            List<InsertStatementProvider<RawRecItem>> inserts = Lists.newArrayList();
            recItems.forEach(item -> {
                if (queryByUniqueFlag(item.getProject(), item.getModelID(), item.getUniqueFlag(),
                        item.getSemanticVersion()) != null) {
                    updaters.add(getUpdateProvider(item));
                } else {
                    inserts.add(getInsertProvider(item));
                }
            });
            updaters.forEach(mapper::update);
            inserts.forEach(mapper::insert);
            session.commit();
        }
    }

    public void deleteByProject(String project) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(table)//
                    .where(table.project, isEqualTo(project)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int rows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row(s) raw recommendation takes {} ms for project [{}]", rows,
                    System.currentTimeMillis() - startTime, project);
        }
    }

    public void cleanForDeletedProject(List<String> projectList) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(table)//
                    .where(table.project, isNotIn(projectList)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int rows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row(s) residual raw recommendation takes {} ms", rows,
                    System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            log.error("Fail to clean raw rec for deleted project ", e);
        }
    }

    public void deleteBySemanticVersion(int semanticVersion, String model) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(table)//
                    .where(table.semanticVersion, isLessThan(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int rows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row(s) raw recommendation takes {} ms", rows, System.currentTimeMillis() - startTime);
        }
    }

    public void discardRecItemsOfBrokenModel(String model) {
        long startTime = System.currentTimeMillis();
        RawRecItem.RawRecType[] rawRecTypes = { RawRecItem.RawRecType.ADDITIONAL_LAYOUT,
                RawRecItem.RawRecType.REMOVAL_LAYOUT };
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            UpdateStatementProvider updateProvider = SqlBuilder.update(table)//
                    .set(table.updateTime).equalTo(startTime)//
                    .set(table.state).equalTo(RawRecItem.RawRecState.DISCARD)//
                    .where(table.modelID, isEqualTo(model)) //
                    .and(table.type, isIn(rawRecTypes)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int rows = mapper.update(updateProvider);
            session.commit();
            log.info("Discard {} row(s) raw recommendation of broken model takes {} ms", rows,
                    System.currentTimeMillis() - startTime);
        }
    }

    public void deleteRecItemsOfNonExistModels(String project, Set<String> existingModels) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(table)//
                    .where(table.project, isEqualTo(project)) //
                    .and(table.modelID, isNotIn(existingModels)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int rows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row(s) raw recommendation takes {} ms", rows, System.currentTimeMillis() - startTime);
        }
    }

    public int getRecItemCountByProject(String project, RawRecItem.RawRecType type) {
        SelectStatementProvider statementProvider = select(count(table.id)) //
                .from(table) //
                .where(table.project, isEqualTo(project)) //
                .and(table.type, isEqualTo(type)) //
                .build().render(RenderingStrategies.MYBATIS3);
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            return mapper.selectAsInt(statementProvider);
        }
    }

    public void updateAllCost(String project) {
        final int batchToUpdate = 1000;
        long currentTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            // if no records, no need to update cost
            if (mapper.selectAsInt(getContStarProvider()) == 0) {
                return;
            }

            int totalUpdated = 0;
            for (int i = 0;; i++) {
                List<RawRecItem> rawRecItems = mapper.selectMany(getSelectLayoutProvider(project, batchToUpdate,
                        batchToUpdate * i, RawRecItem.RawRecState.INITIAL, RawRecItem.RawRecState.RECOMMENDED));
                int size = rawRecItems.size();
                updateCost(currentTime, session, mapper, rawRecItems);
                totalUpdated += size;
                if (size < batchToUpdate) {
                    break;
                }
            }
            log.info("Update the cost of all {} raw recommendation takes {} ms", totalUpdated,
                    System.currentTimeMillis() - currentTime);
        }
    }

    private void updateCost(long currentTime, SqlSession session, RawRecItemMapper mapper, List<RawRecItem> oneBatch) {
        if (oneBatch.isEmpty()) {
            return;
        }

        oneBatch.forEach(recItem -> {
            if (recItem.getRecSource().equalsIgnoreCase(RawRecItem.IMPORTED)) {
                recItem.setUpdateTime(currentTime);
                return;
            }
            LayoutMetric.LatencyMap latencyMap = recItem.getLayoutMetric().getLatencyMap();
            recItem.setTotalLatencyOfLastDay(latencyMap.getLatencyByDate(System.currentTimeMillis() - MILLIS_PER_DAY));
            recItem.setCost((recItem.getCost() + recItem.getTotalLatencyOfLastDay()) / Math.E);
            recItem.setUpdateTime(currentTime);
        });
        List<UpdateStatementProvider> providers = Lists.newArrayList();
        oneBatch.forEach(item -> providers.add(getUpdateProvider(item)));
        providers.forEach(mapper::update);
        session.commit();
        oneBatch.clear();
    }

    private SelectStatementProvider getSelectLayoutProvider(String project, int limit, int offset,
            RawRecItem.RawRecState... states) {
        return select(getSelectFields(table)) //
                .from(table).where(table.project, isEqualTo(project)) //
                .and(table.type, isEqualTo(RawRecItem.RawRecType.ADDITIONAL_LAYOUT)) //
                .and(table.state, isIn(states)) //
                .limit(limit).offset(offset) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getMinIdProvider() {
        return select(min(table.id)).from(table).build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getMaxIdProvider() {
        return select(max(table.id)).from(table).build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getContStarProvider() {
        return select(count(table.id)).from(table).build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getSelectByIdStatementProvider(int id) {
        return select(getSelectFields(table)) //
                .from(table) //
                .where(table.id, isEqualTo(id)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getSelectUniqueFlagIdStatementProvider(String project, String modelId, String uniqueFlag,
            Integer semanticVersion) {
        return select(getSelectFields(table)) //
                .from(table) //
                .where(table.uniqueFlag, isEqualTo(uniqueFlag)) //
                .and(table.project, isEqualTo(project)) //
                .and(table.modelID, isEqualTo(modelId)) //
                .and(table.semanticVersion, isEqualTo(semanticVersion)).build().render(RenderingStrategies.MYBATIS3);
    }

    InsertStatementProvider<RawRecItem> getInsertProvider(RawRecItem recItem) {
        return SqlBuilder.insert(recItem).into(table).map(table.project).toProperty("project") //
                .map(table.modelID).toProperty("modelID") //
                .map(table.uniqueFlag).toProperty("uniqueFlag") //
                .map(table.semanticVersion).toProperty("semanticVersion") //
                .map(table.type).toProperty("type") //
                .map(table.recEntity).toProperty("recEntity") //
                .map(table.state).toProperty("state") //
                .map(table.createTime).toProperty("createTime") //
                .map(table.updateTime).toProperty("updateTime") //
                .map(table.dependIDs).toPropertyWhenPresent("dependIDs", recItem::getDependIDs) //
                // only for layout raw recommendation
                .map(table.layoutMetric).toPropertyWhenPresent("layoutMetric", recItem::getLayoutMetric) //
                .map(table.cost).toPropertyWhenPresent("cost", recItem::getCost) //
                .map(table.totalLatencyOfLastDay)
                .toPropertyWhenPresent("totalLatencyOfLastDay", recItem::getTotalLatencyOfLastDay) //
                .map(table.hitCount).toPropertyWhenPresent("hitCount", recItem::getHitCount) //
                .map(table.totalTime).toPropertyWhenPresent("totalTime", recItem::getMaxTime) //
                .map(table.maxTime).toPropertyWhenPresent("maxTime", recItem::getMinTime) //
                .map(table.minTime).toPropertyWhenPresent("minTime", recItem::getMinTime) //
                .map(table.queryHistoryInfo).toPropertyWhenPresent("queryHistoryInfo", recItem::getQueryHistoryInfo) //
                .map(table.recSource).toPropertyWhenPresent("recSource", recItem::getRecSource).build()
                .render(RenderingStrategies.MYBATIS3);
    }

    UpdateStatementProvider changeRecStateProvider(int id, RawRecItem.RawRecState state, long updateTime) {
        return SqlBuilder.update(table) //
                .set(table.state).equalTo(state) //
                .set(table.updateTime).equalTo(updateTime) //
                .where(table.id, isEqualTo(id)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    UpdateStatementProvider getUpdateProvider(RawRecItem recItem) {
        return SqlBuilder.update(table)//
                .set(table.uniqueFlag).equalTo(recItem::getUniqueFlag) //
                .set(table.semanticVersion).equalTo(recItem::getSemanticVersion) //
                .set(table.state).equalTo(recItem::getState) //
                .set(table.updateTime).equalTo(recItem::getUpdateTime) //
                .set(table.recEntity).equalTo(recItem::getRecEntity) //
                .set(table.dependIDs).equalToWhenPresent(recItem::getDependIDs) //
                // only for layout raw recommendation
                .set(table.layoutMetric).equalToWhenPresent(recItem::getLayoutMetric) //
                .set(table.cost).equalToWhenPresent(recItem::getCost) //
                .set(table.totalLatencyOfLastDay).equalToWhenPresent(recItem::getTotalLatencyOfLastDay) //
                .set(table.hitCount).equalToWhenPresent(recItem::getHitCount) //
                .set(table.totalTime).equalToWhenPresent(recItem::getTotalTime) //
                .set(table.maxTime).equalToWhenPresent(recItem::getMaxTime) //
                .set(table.minTime).equalToWhenPresent(recItem::getMinTime) //
                .set(table.queryHistoryInfo).equalToWhenPresent(recItem::getQueryHistoryInfo) //
                .set(table.recSource).equalToWhenPresent(recItem::getRecSource) //
                .where(table.id, isEqualTo(recItem::getId)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private BasicColumn[] getSelectFields(RawRecItemTable recItemTable) {
        return BasicColumn.columnList(//
                recItemTable.id, //
                recItemTable.project, //
                recItemTable.modelID, //
                recItemTable.uniqueFlag, //
                recItemTable.semanticVersion, //
                recItemTable.type, //
                recItemTable.recEntity, //
                recItemTable.dependIDs, //
                recItemTable.state, //
                recItemTable.createTime, //
                recItemTable.updateTime, //

                // only for layout
                recItemTable.layoutMetric, //
                recItemTable.cost, //
                recItemTable.totalLatencyOfLastDay, //
                recItemTable.hitCount, //
                recItemTable.totalTime, //
                recItemTable.maxTime, //
                recItemTable.minTime, //
                recItemTable.queryHistoryInfo, //
                recItemTable.recSource);
    }

    public int getMaxId() {
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            if (mapper.selectAsInt(getContStarProvider()) == 0) {
                return 0;
            }
            return mapper.selectAsInt(getMaxIdProvider());
        }
    }
}