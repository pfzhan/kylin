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

package io.kyligence.kap.query;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;

import io.kyligence.kap.common.extension.ExtensionFactoryLoader;
import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.query.engine.SchemaMapExtension;
import io.kyligence.kap.query.engine.TableColumnAuthExtension;

public class QueryExtension {

    private static Factory extensionFactory = null;
    private static final ExtensionFactoryLoader<Factory> loader = new ExtensionFactoryLoader<>();

    private QueryExtension() {
    }

    public static Factory getFactory() {
        if (extensionFactory == null) {
            synchronized (QueryExtension.class) {
                extensionFactory = loader.loadFactory(Factory.class,
                        KylinConfig.getInstanceFromEnv().getQueryExtensionFactory());
            }
        }
        return extensionFactory;
    }

    @VisibleForTesting
    public static void setFactory(Factory newFactory) {
        synchronized (QueryExtension.class) {
            extensionFactory = newFactory;
        }
    }

    public static class Factory {

        public Factory() {
            // Do nothing
        }

        private SchemaMapExtension schemaMapExtension;
        private TableColumnAuthExtension tableColumnAuthExtension;

        public SchemaMapExtension getSchemaMapExtension() {
            if (schemaMapExtension == null) {
                schemaMapExtension = createSchemaMapExtension();
            }
            return schemaMapExtension;
        }

        public TableColumnAuthExtension getTableColumnAuthExtension() {
            if (tableColumnAuthExtension == null) {
                tableColumnAuthExtension = createTableColumnAuthExtension();
            }
            return tableColumnAuthExtension;
        }

        // ------------------------- private method

        protected SchemaMapExtension createSchemaMapExtension() {
            return new SchemaMapExtensionDefault();
        }

        protected TableColumnAuthExtension createTableColumnAuthExtension() {
            return new TableColumnAuthExtensionDefault();
        }
    }


    // ========================= Default Impl of QueryExtension


    private static class SchemaMapExtensionDefault implements SchemaMapExtension {
        @Override
        public Map<String, List<TableDesc>> getAuthorizedTablesAndColumns(KylinConfig kylinConfig, String projectName,
                                                                          boolean fullyAuthorized, String userName, Set<String> groups) {
            return NTableMetadataManager.getInstance(kylinConfig, projectName).listTablesGroupBySchema();
        }
    }

    private static class TableColumnAuthExtensionDefault implements TableColumnAuthExtension {
        @Override
        public boolean isColumnsAuthorized(KylinConfig kylinConfig, String projectName, String user, Set<String> groups, Set<String> columns) {
            return true;
        }
    }
}
