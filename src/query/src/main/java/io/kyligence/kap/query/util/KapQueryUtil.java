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

package io.kyligence.kap.query.util;

import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;

public class KapQueryUtil {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(KapQueryUtil.class);

    private static List<IPushDownConverter> queryTransformers;
    
    public static String massageComputedColumn(String origin, String project, String defaultSchema, NDataModel model) {
        String transformed = origin;
        try {
            // massage nested CC for drafted model
            Map<String, NDataModel> modelMap = Maps.newHashMap();
            modelMap.put(model.getName(), model);
            transformed = RestoreFromComputedColumn.convertWithGivenModels(transformed, project, defaultSchema,
                    modelMap);
        } catch (Exception e) {
            LOGGER.warn("Failed to massage SQL expression [{}] with input model {}", origin, model.getName(), e);
            return origin;
        }
        return massageComputedColumn(transformed, project, defaultSchema);
    }

    public static String massageComputedColumn(String origin, String project, String defaultSchema) {
        String transformed = origin;

        if (queryTransformers == null) {
            queryTransformers = Lists.newArrayList();
            String[] classes = KylinConfig.getInstanceFromEnv().getPushDownConverterClassNames();
            for (String clz : classes) {
                try {
                    IPushDownConverter t = (IPushDownConverter) ClassUtil.newInstance(clz);
                    if (t instanceof SparkSQLFunctionConverter) {
                        // TODO adjust dialect by data types
                        ((EscapeTransformer) t).setFunctionDialect(EscapeDialect.HIVE);
                    }
                    queryTransformers.add(t);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to init query transformer", e);
                }
            }
        }

        try {
            for (IPushDownConverter t : queryTransformers) {
                transformed = t.convert(transformed, project, defaultSchema, false);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to massage SQL expression [{}] with pushdown converters", origin, e);
            return origin;
        }

        return transformed;
    }
    
    public static SqlSelect extractSqlSelect(SqlCall selectOrOrderby) {
        SqlSelect sqlSelect = null;

        if (selectOrOrderby instanceof SqlSelect) {
            sqlSelect = (SqlSelect) selectOrOrderby;
        } else if (selectOrOrderby instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = ((SqlOrderBy) selectOrOrderby);
            if (sqlOrderBy.query instanceof SqlSelect) {
                sqlSelect = (SqlSelect) sqlOrderBy.query;
            }
        }
        
        return sqlSelect;
    }
}
