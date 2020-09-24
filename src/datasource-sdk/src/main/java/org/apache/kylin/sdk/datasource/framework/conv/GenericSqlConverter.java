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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.sdk.datasource.framework.def.DataSourceDef;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDefProvider;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class GenericSqlConverter {

    private final Cache<String, SqlConverter> SQL_CONVERTER_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS).maximumSize(30).build();

    public String convertSql(String originSql, String sourceDialect, String targetDialect) throws SQLException {
        SqlConverter sqlConverter = getSqlConverter(sourceDialect, targetDialect);
        return sqlConverter.convertSql(originSql);
    }

    private SqlConverter getSqlConverter(String sourceDialect, String targetDialect) throws SQLException {
        String cacheKey = sourceDialect + "_" + targetDialect;
        SqlConverter sqlConverter = SQL_CONVERTER_CACHE.getIfPresent(cacheKey);
        if (sqlConverter == null) {
            sqlConverter = createSqlConverter(sourceDialect, targetDialect);
            SQL_CONVERTER_CACHE.put(cacheKey, sqlConverter);
        }
        return sqlConverter;
    }

    private SqlConverter createSqlConverter(String sourceDialect, String targetDialect) throws SQLException {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        DataSourceDef sourceDs = provider.getById(sourceDialect);
        final DataSourceDef targetDs = provider.getById(targetDialect);
        ConvMaster convMaster = new ConvMaster(sourceDs, targetDs);
        SqlConverter.IConfigurer configurer = new DefaultConfigurer(targetDs);
        return new SqlConverter(configurer, convMaster);
    }
}
