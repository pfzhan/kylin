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

package io.kyligence.kap.metadata.state;

import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.StorageURL;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.render.UpdateStatementProvider;

import javax.sql.DataSource;
import java.util.Properties;

import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;

@Slf4j
public class JdbcShareStateStore {
    private final ShareStateTable shareStateTable;
    @Getter
    private final SqlSessionFactory sqlSessionFactory;
    private final DataSource dataSource;
    String ssTableName;

    public static JdbcShareStateStore getInstance() {
        return Singletons.getInstance(JdbcShareStateStore.class);
    }

    private JdbcShareStateStore() throws Exception {
        StorageURL url = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        dataSource = JdbcDataSource.getDataSource(props);
        ssTableName = StorageURL.replaceUrl(url) + "_" + "share_state";
        shareStateTable = new ShareStateTable(ssTableName);
        sqlSessionFactory = ShareStateUtil.getSqlSessionFactory(dataSource, ssTableName);
    }

    public int insert(String instanceName, String shareState) {
        try(SqlSession session = sqlSessionFactory.openSession()) {
            ShareStateMapper ssMapper = session.getMapper(ShareStateMapper.class);
            ShareStateInfo shareStateInfoObj = new ShareStateInfo(instanceName, shareState);
            InsertStatementProvider<ShareStateInfo> insertStatement = getInsertShareStateProvider(shareStateInfoObj);
            int rows = ssMapper.insert(insertStatement);
            log.debug("Insert {} items into database, instanceName:{}", rows, instanceName);
            session.commit();
            return rows;
        }
    }

    InsertStatementProvider<ShareStateInfo> getInsertShareStateProvider(ShareStateInfo shareStateInfo) {
        return SqlBuilder.insert(shareStateInfo).into(shareStateTable)
                .map(shareStateTable.instanceName).toPropertyWhenPresent("instanceName", shareStateInfo::getInstanceName) //
                .map(shareStateTable.shareState).toPropertyWhenPresent("shareState", shareStateInfo::getShareState) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    public void update(String instanceName, String shareState) {
        try(SqlSession session = sqlSessionFactory.openSession()) {
            ShareStateMapper ssMapper = session.getMapper(ShareStateMapper.class);
            UpdateStatementProvider updateStatement = getUpdateShareStateProvider(instanceName, shareState);
            ssMapper.update(updateStatement);
            session.commit();
        }
    }

    private UpdateStatementProvider getUpdateShareStateProvider(String instanceName, String shareState) {
        return SqlBuilder.update(shareStateTable)
                .set(shareStateTable.shareState).equalTo(shareState)
                .where(shareStateTable.instanceName, isEqualTo(instanceName))
                .build().render(RenderingStrategies.MYBATIS3);
    }

    public ShareStateInfo selectShareStateByInstanceName(String instanceName) {
        try(SqlSession session = sqlSessionFactory.openSession()) {
            ShareStateMapper ssMapper = session.getMapper(ShareStateMapper.class);
            SelectStatementProvider selectStatement = getSelectShareStateProvider(instanceName);
            return ssMapper.selectOne(selectStatement);
        }
    }

    private SelectStatementProvider getSelectShareStateProvider(String instanceName) {
        return SqlBuilder.select(getSelectFields(shareStateTable))
                .from(shareStateTable)
                .where(shareStateTable.instanceName, isEqualTo(instanceName))
                .limit(1)
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private BasicColumn[] getSelectFields(ShareStateTable shareStateTable) {
        return BasicColumn.columnList(shareStateTable.instanceName, shareStateTable.shareState);
    }

}
