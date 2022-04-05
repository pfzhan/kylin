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

package io.kyligence.kap.metadata;

import com.google.common.collect.Sets;
import io.kyligence.kap.common.extension.ExtensionFactoryLoader;
import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import io.kyligence.kap.metadata.query.QueryExcludedTablesExtension;
import org.apache.kylin.common.KylinConfig;

import java.util.Set;

public class MetadataExtension {

    private static Factory extensionFactory = null;
    private static final ExtensionFactoryLoader<Factory> loader = new ExtensionFactoryLoader<>();

    private MetadataExtension() {
    }

    public static Factory getFactory() {
        if (extensionFactory == null) {
            synchronized (MetadataExtension.class) {
                extensionFactory = loader.loadFactory(Factory.class,
                        KylinConfig.getInstanceFromEnv().getMetadataExtensionFactory());
            }
        }
        return extensionFactory;
    }

    @VisibleForTesting
    public static void setFactory(Factory newFactory) {
        synchronized (MetadataExtension.class) {
            extensionFactory = newFactory;
        }
    }

    public static class Factory {

        public Factory() {
            // Do nothing
        }

        private QueryExcludedTablesExtension queryExcludedTablesExtension;

        public final QueryExcludedTablesExtension getQueryExcludedTablesExtension() {
            if (queryExcludedTablesExtension == null) {
                queryExcludedTablesExtension = createQueryExcludedTablesExtension();
            }
            return queryExcludedTablesExtension;
        }

        protected QueryExcludedTablesExtension createQueryExcludedTablesExtension() {
            return new QueryExcludedTablesExtensionDefault();
        }
    }

    private static class QueryExcludedTablesExtensionDefault implements QueryExcludedTablesExtension {
        @Override
        public Set<String> getExcludedTables(KylinConfig kylinConfig, String projectName) {
            return Sets.newHashSet();
        }

        @Override
        public void addExcludedTables(KylinConfig config, String projectName, String tableName, boolean isEnabled) {
            // do nothing
        }
    }
}