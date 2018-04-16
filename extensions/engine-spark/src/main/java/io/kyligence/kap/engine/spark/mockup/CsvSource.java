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

package io.kyligence.kap.engine.spark.mockup;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.kyligence.kap.engine.spark.NSparkCubingEngine.NSparkCubingSource;
import io.kyligence.kap.metadata.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;

public class CsvSource implements ISource {

    @Override
    public ISourceMetadataExplorer getSourceMetadataExplorer() {
        return new ISourceMetadataExplorer() {

            List<ProjectInstance> allProjects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .listAllProjects();

            @Override
            public List<String> listDatabases() {
                Set<String> databases = new TreeSet<>();
                for (ProjectInstance prj : allProjects) {
                    NTableMetadataManager mgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                            prj.getName());
                    for (TableDesc tbl : mgr.listAllTables()) {
                        databases.add(tbl.getDatabase());
                    }
                }
                return new ArrayList<String>(databases);
            }

            @Override
            public List<String> listTables(String database) {
                Set<String> tables = new TreeSet<>();
                for (ProjectInstance prj : allProjects) {
                    NTableMetadataManager mgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                            prj.getName());

                    for (TableDesc tbl : mgr.listAllTables()) {
                        if (database.equals(tbl.getDatabase()))
                            tables.add(tbl.getName());
                    }
                }
                return new ArrayList<String>(tables);
            }

            @Override
            public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String table, String prj) {
                NTableMetadataManager mgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), prj);
                String tableName = database + "." + table;
                TableDesc tableDesc = mgr.getTableDesc(tableName);
                TableExtDesc tableExt = mgr.getTableExt(tableName);
                return Pair.newPair(tableDesc, tableExt);
            }

            @Override
            public List<String> getRelatedKylinResources(TableDesc table) {
                return Collections.emptyList();
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {

        if (engineInterface == NSparkCubingSource.class) {
            return (I) new NSparkCubingSource() {

                @Override
                public Dataset<Row> getSourceData(TableDesc table, SparkSession ss) {
                    String path = new File(getUtMetaDir(), "data/" + table.getIdentity() + ".csv").getAbsolutePath();

                    ColumnDesc[] columnDescs = table.getColumns();
                    String[] colNames = new String[columnDescs.length];
                    for (int i = 0; i < columnDescs.length; i++) {
                        colNames[i] = columnDescs[i].getName();
                    }
                    return ss.read().csv(path).toDF(colNames);
                }
            };
        }
        throw new IllegalArgumentException("Unsupported engine interface: " + engineInterface);
    }

    @Override
    public IReadableTable createReadableTable(TableDesc tableDesc) {
        return new CsvTable(getUtMetaDir(), tableDesc);
    }

    @Override
    public SegmentRange enrichSourcePartitionBeforeBuild(IBuildable buildable, SegmentRange srcPartition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ISampleDataDeployer getSampleDataDeployer() {
        throw new UnsupportedOperationException();
    }

    private String getUtMetaDir() {
        // this is only meant to be used in UT
        final String utMetaDir = System.getProperty(KylinConfig.KYLIN_CONF);
        if (utMetaDir == null || !utMetaDir.startsWith("../example"))
            throw new IllegalStateException();
        return utMetaDir;
    }
}
