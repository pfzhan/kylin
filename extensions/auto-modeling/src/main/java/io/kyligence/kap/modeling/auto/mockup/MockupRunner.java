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

package io.kyligence.kap.modeling.auto.mockup;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.calcite.jdbc.Driver;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.cuboid.CuboidCLI;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.enumerator.LookupTableEnumerator;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import io.kyligence.kap.modeling.auto.mockup.QueryResult.OLAPQueryResult;
import io.kyligence.kap.modeling.auto.proposer.AnalyticsDomain;

public class MockupRunner {

    private static final Logger logger = LoggerFactory.getLogger(MockupRunner.class);

    public static void main(String[] args) {
        // LOG4J setting
        String DEFAULT_PATTERN_LAYOUT = "L4J [%d{yyyy-MM-dd HH:mm:ss,SSS}][%p][%c] - %m%n";
        org.apache.log4j.BasicConfigurator.configure(new ConsoleAppender(new PatternLayout(DEFAULT_PATTERN_LAYOUT)));

        // Testing args
        String metadataDir = "/Users/yifanzhang/Documents/WorkZone@Me/KAP/meta_2016_12_15_05_33_09";
        String project = "learn_kylin";
        String[] sqls = { "select LSTG_FORMAT_NAME, SELLER_ID, PART_DT, USER_ID, REGION from kylin_sales " + "where region is not null and part_dt >= '2010-10-10'", "select week_beg_dt from kylin_sales inner join kylin_cal_dt on part_dt = cal_dt" };
        // User args
        if (args.length > 0) {
            metadataDir = args[0];
            File sqlFolder = new File(metadataDir + "/sql");
            if (sqlFolder.exists() && sqlFolder.isDirectory()) {
                List<String> queries = new ArrayList<>();
                for (File sqlFile : FileUtils.listFiles(sqlFolder, new String[] { "sql" }, false)) {
                    try {
                        String query = FileUtils.readFileToString(sqlFile, Charset.defaultCharset()).trim();
                        if (!StringUtils.isEmpty(query)) {
                            queries.add(query);
                        }
                    } catch (IOException e) {
                        logger.warn("Cannot read file: " + sqlFile.getAbsolutePath());
                    }
                }
                if (!queries.isEmpty()) {
                    sqls = queries.toArray(new String[0]);
                }
            }
        }
        if (args.length > 1) {
            project = args[1];
        }

        KylinConfig config = preparedConfig();
        config.setMetadataUrl(new File(metadataDir).getAbsolutePath());

        try {
            Class.forName(Driver.class.getName());
        } catch (ClassNotFoundException e) {
            logger.error("Failed to create OLAP Schema: {}", e.getMessage());
        }

        try {
            File input = new File(metadataDir, "input.json");
            CubeDesc srcCubeDesc = JsonUtil.readValue(input, CubeDesc.class);
            CubeDesc dstCubeDesc = pruneCube(config, project, srcCubeDesc, sqls);
            String cubeJson = JsonUtil.writeValueAsIndentString(dstCubeDesc);
            File output = new File(metadataDir, "output.json");
            FileUtils.writeStringToFile(output, cubeJson, Charset.defaultCharset(), false);

            // Re-check
            // CubeDesc srcCubeDesc2 = CubeDesc.getCopyOf(dstCubeDesc);
            // CubeDesc dstCubeDesc2 = pruneCube(config, project, srcCubeDesc2,
            // sqls);
            // System.out.println(JsonUtil.writeValueAsString(dstCubeDesc2));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static CubeDesc pruneCube(KylinConfig config, String project, CubeDesc srcCubeDesc, String[] sqls) throws IOException, ClassNotFoundException, SQLException {
        CubeDescManager cubeDescManager = CubeDescManager.getInstance(config);
        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance oldCube = cubeManager.getCube(srcCubeDesc.getName());
        if (oldCube != null) {
            cubeManager.dropCube(oldCube.getName(), true);
        }
        CubeDesc createdCubeDesc = cubeDescManager.createCubeDesc(srcCubeDesc);
        if (createdCubeDesc.getError().size() > 0) {
            throw new IllegalStateException(Joiner.on("\n").join(createdCubeDesc.getError()));
        }
        int cuboidCount = CuboidCLI.simulateCuboidGeneration(createdCubeDesc, false);
        logger.info("New cube " + srcCubeDesc.getName() + " has " + cuboidCount + " cuboids");
        // TODO set owner
        CubeInstance createdCube = cubeManager.createCube(createdCubeDesc.getName(), project, createdCubeDesc, "");
        CubeUpdate cubeUpdate = new CubeUpdate(createdCube);
        cubeUpdate.setStatus(RealizationStatusEnum.READY);
        cubeManager.updateCube(cubeUpdate);
        List<Long> hotCuboids = new ArrayList<>(sqls.length);
        Set<FunctionDesc> measureFuncs = new HashSet<>();
        for (String sql : sqls) {
            QueryResult result = dryRunSQL(sql, project, sql);
            for (OLAPQueryResult olapResult : result.getOLAPQueries()) {
                hotCuboids.add(olapResult.getCuboidId());
                measureFuncs.addAll(olapResult.getMeasures());
            }
        }

        if (hotCuboids.size() == 0) {
            logger.info("No valid dry run info collected, return intial cube.");
            return srcCubeDesc;
        }

        Long pruneCuboid = 0L;
        for (Long cuboid : hotCuboids) {
            pruneCuboid |= cuboid;
        }
        System.out.println(pruneCuboid);

        RowKeyColDesc[] rowKeyCols = srcCubeDesc.getRowkey().getRowKeyColumns();
        long filter = pruneCuboid.longValue();
        List<TblColRef> dimensionCols = new ArrayList<>();
        for (int i = 0; filter > 0 && i < rowKeyCols.length; i++) {
            if ((filter & 1) == 1) {
                dimensionCols.add(rowKeyCols[rowKeyCols.length - i - 1].getColRef());
            }
            filter >>= 1;
        }

        AnalyticsDomain domain = new AnalyticsDomain();
        domain.setModel(srcCubeDesc.getModel());
        domain.setDimensionCols(dimensionCols);
        domain.setMeasureFuncs(measureFuncs);

        CubeDesc prunedCube = new MockupCubeBuilder(domain).build();
        return prunedCube;
    }

    private synchronized static QueryResult dryRunSQL(String uuid, String project, String sql) throws SQLException, ClassNotFoundException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        // Properties props = KylinConfig.getKylinProperties();
        Properties props = new Properties();
        props.setProperty("scan_threshold", "10000000");
        config.setProperty("kylin.storage.url", uuid);

        ResultSet resultSet = null;

        File olapTmp = OLAPSchemaFactory.createTempOLAPJson(project, config);

        try (Connection cubeConnection = DriverManager.getConnection("jdbc:calcite:model=" + olapTmp.getAbsolutePath(), props); Statement statement = cubeConnection.createStatement();) {
            sql = QueryMassagist.massageSql(sql);
            resultSet = statement.executeQuery(sql);
        } catch (Exception e) {
            logger.error("Query Error:", e);
            if (e.getCause() == null) {
                throw e;
            }
            if (e.getCause() instanceof NullPointerException) {
                StackTraceElement[] stackTrace = e.getCause().getStackTrace();
                for (StackTraceElement s : stackTrace) {
                    if (s.toString().contains(LookupTableEnumerator.class.getName())) {
                        logger.info("Skip dry run because this query only hits lookup tables.");
                        return null;
                    }
                }
            }
        } finally {
            // closeQuietly
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                // ignore
            }
        }

        QueryResult result = MockupQueryRecorder.getAndRemoveQueryResult(uuid);
        FileUtils.deleteQuietly(olapTmp);
        return result;
    }

    public static KylinConfig preparedConfig() {
        KylinConfig.destroyInstance();
        Properties props = new Properties();
        props.setProperty("kylin.storage.engine.70", "io.kyligence.kap.modeling.auto.mockup.MockupStorage");
        props.setProperty("kylin.cube.aggrgroup.max-combination", Integer.toString(Integer.MAX_VALUE - 1));
        KylinConfig.setKylinConfigInEnvIfMissing(props);
        return KylinConfig.getInstanceFromEnv();
    }
}
