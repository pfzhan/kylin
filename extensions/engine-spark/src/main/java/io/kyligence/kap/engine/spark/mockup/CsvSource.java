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
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourcePartition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.kyligence.kap.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.engine.spark.NJoinedFlatTable;
import io.kyligence.kap.engine.spark.NSparkCubingEngine.NSparkCubingSource;

public class CsvSource implements ISource {

    @Override
    public ISourceMetadataExplorer getSourceMetadataExplorer() {
        return new ISourceMetadataExplorer() {

            List<ProjectInstance> allProjects = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .listAllProjects();
            TableMetadataManager mgr = TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv());

            @Override
            public List<String> listDatabases() throws Exception {
                Set<String> databases = new TreeSet<>();
                for (ProjectInstance prj : allProjects) {
                    for (TableDesc tbl : mgr.listAllTables(prj.getName())) {
                        databases.add(tbl.getDatabase());
                    }
                }
                return new ArrayList<String>(databases);
            }

            @Override
            public List<String> listTables(String database) throws Exception {
                Set<String> tables = new TreeSet<>();
                for (ProjectInstance prj : allProjects) {
                    for (TableDesc tbl : mgr.listAllTables(prj.getName())) {
                        if (database.equals(tbl.getDatabase()))
                            tables.add(tbl.getName());
                    }
                }
                return new ArrayList<String>(tables);
            }

            @Override
            public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String table, String prj)
                    throws Exception {
                String tableName = database + "." + table;
                TableDesc tableDesc = mgr.getTableDesc(tableName, prj);
                TableExtDesc tableExt = mgr.getTableExt(tableName, prj);
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
        // this is only meant to be used in UT
        final String utMetaDir = System.getProperty(KylinConfig.KYLIN_CONF);
        if (utMetaDir == null || utMetaDir.startsWith("../example") == false)
            throw new IllegalStateException();

        if (engineInterface == NSparkCubingSource.class) {
            return (I) new NSparkCubingSource() {

                @Override
                public Dataset<Row> getSourceData(NDataflow dataflow, @SuppressWarnings("rawtypes") SegmentRange range,
                        SparkSession ss) {
                    NCubeJoinedFlatTableDesc flatTable = new NCubeJoinedFlatTableDesc(dataflow.getCubePlan(), range);
                    return NJoinedFlatTable.generateDataset(flatTable, this, ss);
                }

                @Override
                public Dataset<Row> getSourceData(TableDesc table, SparkSession ss) {
                    String path = new File(utMetaDir, "data/" + table.getIdentity() + ".csv").getAbsolutePath();

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
        throw new UnsupportedOperationException();
    }

    @Override
    public SourcePartition enrichSourcePartitionBeforeBuild(IBuildable buildable, SourcePartition srcPartition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ISampleDataDeployer getSampleDataDeployer() {
        throw new UnsupportedOperationException();
    }

}
