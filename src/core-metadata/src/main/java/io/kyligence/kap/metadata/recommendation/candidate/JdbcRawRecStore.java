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

import static org.mybatis.dynamic.sql.SqlBuilder.count;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isIn;
import static org.mybatis.dynamic.sql.SqlBuilder.isLessThan;
import static org.mybatis.dynamic.sql.SqlBuilder.isNotEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.max;
import static org.mybatis.dynamic.sql.SqlBuilder.min;
import static org.mybatis.dynamic.sql.SqlBuilder.select;

import java.util.List;
import java.util.Properties;

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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.util.RawRecStoreUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcRawRecStore {

    private static final String RECOMMENDATION_CANDIDATE = "_rec_candidate";

    private final RawRecItemTable table;
    @VisibleForTesting
    @Getter
    private final SqlSessionFactory sqlSessionFactory;

    public JdbcRawRecStore(KylinConfig config) throws Exception {
        StorageURL url = config.getMetadataUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        DataSource dataSource = JdbcDataSource.getDataSource(props);
        table = new RawRecItemTable(url.getIdentifier() + RECOMMENDATION_CANDIDATE);
        sqlSessionFactory = RawRecStoreUtil.getSqlSessionFactory(dataSource, table.tableNameAtRuntime());
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
            log.info("List all recommendations on project({})/model({}, {}) takes {} ms.", project, model,
                    semanticVersion, System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    public List<RawRecItem> getAllLayoutCandidates(String project, String model) {
        int semanticVersion = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDesc(model).getSemanticVersion();
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(RawRecItem.RawRecType.LAYOUT)) //
                    .and(table.state, isEqualTo(RawRecItem.RawRecState.RECOMMENDED)).build()
                    .render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> rawRecItems = mapper.selectMany(statementProvider);
            log.info("List existing raw recommendations on project({})/model({}, {}) takes {} ms.", project, model,
                    semanticVersion, System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    public List<RawRecItem> chooseTopNCandidates(String project, String model, int topN, RawRecItem.RawRecState state) {
        int semanticVersion = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDesc(model).getSemanticVersion();
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(RawRecItem.RawRecType.LAYOUT)) //
                    .and(table.state, isEqualTo(state)) //
                    .orderBy(table.cost.descending(), table.hitCount.descending(), table.id.descending()) //
                    .limit(topN) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> rawRecItems = mapper.selectMany(statementProvider);
            log.info("List topN({}) recommendations on project({})/model({}, {}) takes {} ms.", topN, project, model,
                    semanticVersion, System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    public List<RawRecItem> queryLayoutRawRecItems(String project, String model) {
        long start = System.currentTimeMillis();
        int semanticVersion = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDesc(model).getSemanticVersion();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(RawRecItem.RawRecType.LAYOUT)) //
                    .and(table.state, isNotEqualTo(RawRecItem.RawRecState.APPLIED)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> recItems = mapper.selectMany(statementProvider);
            log.info("Query by raw recommendation type, state takes {} ms", System.currentTimeMillis() - start);
            return recItems;
        }

    }

    public List<RawRecItem> queryByRecTypeAndState(String project, String model, RawRecItem.RawRecType type,
            RawRecItem.RawRecState state) {
        long start = System.currentTimeMillis();
        int semanticVersion = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDesc(model).getSemanticVersion();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(type)) //
                    .and(table.state, isEqualTo(state)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> recItems = mapper.selectMany(statementProvider);
            log.info("Query by raw recommendation type, state takes {} ms", System.currentTimeMillis() - start);
            return recItems;
        }
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

    public void addOrUpdate(RawRecItem recItem) {
        if (recItem.getId() == 0) {
            save(recItem);
        } else {
            long start = System.currentTimeMillis();
            try (SqlSession session = sqlSessionFactory.openSession()) {
                RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
                SelectStatementProvider statementProvider = getSelectByIdStatementProvider(recItem.getId());
                Preconditions.checkState(mapper.selectOne(statementProvider) != null,
                        "There is no raw recommendation with id({})", recItem.getId());
                UpdateStatementProvider updateStatement = getUpdateProvider(recItem);
                int rows = mapper.update(updateStatement);
                session.commit();
                log.info("Update {} raw recommendation(s) takes {} ms", rows, System.currentTimeMillis() - start);
            }
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

    public void updateAllCost(String project) {
        final int batchToUpdate = 1000;
        final int limit = 1000;
        long currentTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            // if no records, no need to update cost
            int count = mapper.selectAsInt(getContStarProvider());
            if (count == 0) {
                return;
            }

            int minId = mapper.selectAsInt(getMinIdProvider());
            int maxId = mapper.selectAsInt(getMaxIdProvider());
            log.info("The min and max value of id is ({}, {})", minId, maxId);

            List<RawRecItem> oneBatch = Lists.newArrayList();
            int step = 1000;
            int i = 0;
            int totalUpdated = 0;
            while (oneBatch.size() < batchToUpdate) {
                SelectStatementProvider selectProvider = getSelectLayoutProvider(project, limit, minId + step * i,
                        RawRecItem.RawRecState.INITIAL, RawRecItem.RawRecState.RECOMMENDED);
                List<RawRecItem> rawRecItems = mapper.selectMany(selectProvider);
                oneBatch.addAll(rawRecItems);
                i++;
                if (oneBatch.size() >= batchToUpdate) {
                    updateCost(currentTime, session, mapper, oneBatch);
                    totalUpdated += oneBatch.size();
                }

                if (minId + step * i > maxId) {
                    break;
                }
            }
            updateCost(currentTime, session, mapper, oneBatch);
            totalUpdated += oneBatch.size();
            log.info("Update the cost of all {} raw recommendation takes {} ms", totalUpdated,
                    System.currentTimeMillis() - currentTime);
        }
    }

    private void updateCost(long currentTime, SqlSession session, RawRecItemMapper mapper, List<RawRecItem> oneBatch) {
        oneBatch.forEach(recItem -> {
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
                .and(table.type, isEqualTo(RawRecItem.RawRecType.LAYOUT)) //
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
                .build().render(RenderingStrategies.MYBATIS3);
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
                recItemTable.queryHistoryInfo);
    }
}
