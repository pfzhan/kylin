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

package io.kyligence.kap.smart;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.job.execution.NExecutableManager;
import io.kyligence.kap.job.impl.threadpool.NDefaultScheduler;
import io.kyligence.kap.metadata.badquery.NBadQueryHistory;
import io.kyligence.kap.metadata.badquery.NBadQueryHistoryManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.routing.Candidate;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.NLocalSparkWithCSVDataTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;

@Ignore("Failed in maven")
public class NE2EDemoTest extends NLocalSparkWithCSVDataTest {
    private KylinConfig kylinConfig;

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        super.setUp();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }

        kylinConfig = getTestConfig();
    }

    @After
    public void after() throws Exception {
        Candidate.restorePriorities();

        NDefaultScheduler.destroyInstance();
        super.tearDown();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    public void testSSB() throws IOException, InterruptedException, SQLException, DatabaseUnitException {
        final String projectName = "ssb";
        final NBadQueryHistoryManager bqhManager = NBadQueryHistoryManager.getInstance(kylinConfig, projectName);

        NBadQueryHistory history = bqhManager.getBadQueriesForProject();
        List<BadQueryEntry> allEntries = Lists.newArrayList(history.getEntries());

        // Round 1 - Use 6 sqls to run e2e scenario
        List<BadQueryEntry> subEntries = allEntries.subList(0, 8);
        history.setEntries(Sets.newTreeSet(subEntries));
        bqhManager.upsertToProject(history);

        // create model and cubes
        NSmartController.optimizeFromPushdown(kylinConfig, projectName);

        // build cubes
        rebuildAllCubes(projectName);

        // query
        queryBadQueries(projectName, subEntries);

        // Round 2 - Use all sqls to run e2e scenario
        history = bqhManager.getBadQueriesForProject();
        history.setEntries(Sets.newTreeSet(allEntries));
        bqhManager.upsertToProject(history);

        // create model and cubes
        NSmartController.optimizeFromPushdown(kylinConfig, projectName);

        // build cubes
        rebuildAllCubes(projectName);

        // query
        queryBadQueries(projectName, allEntries);
    }

    private void rebuildAllCubes(String proj) throws IOException, InterruptedException {
        kylinConfig.clearManagers();
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        NExecutableManager execMgr = NExecutableManager.getInstance(kylinConfig, proj);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, proj);

        List<NSparkCubingJob> jobs = Lists.newArrayList();
        for (IRealization realization : projectManager.listAllRealizations(proj)) {
            NDataflow df = (NDataflow) realization;
            Segments<NDataSegment> readySegments = df.getSegments(SegmentStatusEnum.READY);
            NDataSegment oneSeg = null;
            List<NCuboidLayout> layouts = null;
            if (readySegments.isEmpty()) {
                oneSeg = dataflowManager.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
                layouts = df.getCubePlan().getAllCuboidLayouts();
            } else {
                oneSeg = readySegments.getFirstSegment();
                layouts = Lists.newArrayList();
                for (Map.Entry<Long, NDataCuboid> cuboid : oneSeg.getCuboidsMap().entrySet()) {
                    if (cuboid.getValue().getStatus() == SegmentStatusEnum.NEW)
                        layouts.add(cuboid.getValue().getCuboidLayout());
                }
            }

            // create cubing job
            if (!layouts.isEmpty()) {
                NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts),
                        "ADMIN");
                execMgr.addJob(job);
                jobs.add(job);
            }
        }

        while (true) {
            Thread.sleep(500);
            boolean hasRunning = false;
            for (NSparkCubingJob job : jobs) {
                ExecutableState status = job.getStatus();
                if (status.isReadyOrRunning()) {
                    hasRunning = true;
                } else if (status == ExecutableState.ERROR) {
                    throw new IllegalStateException("Failed to execute job. " + job);
                }
            }

            if (!hasRunning)
                break;
        }
    }

    private void queryBadQueries(String proj, List<BadQueryEntry> entries) throws SQLException, DatabaseUnitException {
        Connection conn = null;
        IDatabaseConnection dbUnitConn = null;

        try {
            conn = QueryConnection.getConnection(proj);
            dbUnitConn = new DatabaseConnection(conn);
            for (BadQueryEntry entry : entries) {
                ITable queryTable = dbUnitConn.createQueryTable(entry.getUuid(), entry.getSql());
                printResult(queryTable);
            }
        } finally {
            if (dbUnitConn != null)
                dbUnitConn.close();

            DBUtils.closeQuietly(conn);
        }
    }

    private void printResult(ITable resultTable) throws DataSetException {
        StringBuilder sb = new StringBuilder();

        int columnCount = resultTable.getTableMetaData().getColumns().length;
        String[] columns = new String[columnCount];

        for (int i = 0; i < columnCount; i++) {
            sb.append(resultTable.getTableMetaData().getColumns()[i].getColumnName());
            sb.append("-");
            sb.append(resultTable.getTableMetaData().getColumns()[i].getDataType());
            sb.append("\t");
            columns[i] = resultTable.getTableMetaData().getColumns()[i].getColumnName();
        }
        sb.append("\n");

        for (int i = 0; i < resultTable.getRowCount(); i++) {
            for (int j = 0; j < columns.length; j++) {
                sb.append(resultTable.getValue(i, columns[j]));
                sb.append("\t");
            }
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }
}
