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

package io.kyligence.kap.tool.smart;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.smart.query.QueryRecord;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.Utils;
import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;

public class QueryCuboidTestCLI {
    public static void main(String[] args) throws Exception {
        //        System.setProperty("log4j.configuration", "file:kap/build/conf/kylin-tools-log4j.properties");
        //        args = new String[2];
        //        args[0] = "TPC_DS_2";
        //        args[1] = "/Users/dong/Projects/bigjohn/kap/tpcds/queries_filtered";
        //        KylinConfig.setKylinConfigThreadLocal(Utils.newKylinConfig("/Users/dong/Projects/bigjohn/kap/tpcds/meta"));

        Logger logger = LoggerFactory.getLogger(QueryCuboidTestCLI.class);

        if (args == null || args.length != 2) {
            System.out.println("Usage: java io.kyligence.kap.tool.smart.QueryCuboidTestCLI <project> <sql_dir>");
            System.out.println("eg. java java io.kyligence.kap.tool.smart.QueryCuboidTestCLI learn_kylin /tmp/sqls");
            System.exit(1);
        }

        String projName = args[0];
        File sqlFile = new File(args[1]);
        Preconditions.checkState(sqlFile.exists(), "SQL File does not exists at " + sqlFile.getAbsolutePath());

        List<String> sqlList = Lists.newArrayList();
        if (sqlFile.isDirectory()) {
            File[] sqlFiles = sqlFile.listFiles();
            Preconditions.checkArgument(sqlFiles != null && sqlFiles.length > 0,
                    "SQL files not found under " + sqlFile.getAbsolutePath());

            for (File file : sqlFiles) {
                sqlList.add(FileUtils.readFileToString(file, Charset.defaultCharset()));
            }
        } else if (sqlFile.isFile()) {
            BufferedReader br = new BufferedReader(new FileReader(sqlFile));
            String line = null;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                if (line.endsWith(";")) {
                    sb.append(line);
                    sb.deleteCharAt(sb.length() - 1);
                    sqlList.add(sb.toString());
                    sb = new StringBuilder();
                } else {
                    sb.append(line);
                    sb.append("\n");
                }
            }
        }

        String[] sqls = sqlList.toArray(new String[0]);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config = Utils.newKylinConfig(config.getMetadataUrl().toString());
        KylinConfig.setKylinConfigThreadLocal(config);

        CubeManager cubeManager = CubeManager.getInstance(config);
        for (CubeInstance cubeInstance : cubeManager.listAllCubes()) {
            cubeInstance.setStatus(RealizationStatusEnum.READY);
            if (cubeInstance.getSegments().isEmpty()) {
                CubeSegment mockSeg = new CubeSegment();
                mockSeg.setUuid(UUID.randomUUID().toString());
                mockSeg.setStorageLocationIdentifier(UUID.randomUUID().toString());
                ConcurrentHashMap<String, String> snapshots = new ConcurrentHashMap<>();
                for (TableRef tblRef : cubeInstance.getModel().getLookupTables()) {
                    snapshots.put(tblRef.getTableIdentity(), "");
                }
                mockSeg.setSnapshots(snapshots);
                mockSeg.setStatus(SegmentStatusEnum.READY);
                mockSeg.setCubeInstance(cubeInstance);
                cubeInstance.getSegments().add(mockSeg);
            }
            cubeManager.updateCube(new CubeUpdate(cubeInstance));
        }
        MockupQueryExecutor exec = new MockupQueryExecutor();
        Map<String, QueryRecord> failed = Maps.newHashMap();
        Map<String, Pair<Long, Long>> cuboidUnmatch = Maps.newHashMap();
        for (String sql : sqls) {
            QueryRecord rec = exec.execute(projName, sql);
            if (rec.getSqlResult().getStatus() == SQLResult.Status.FAILED) {
                failed.put(sql, rec);
            } else if (rec.getCubeInstance() != null) {
                CubeDesc cubeDesc = rec.getCubeInstance().getDescriptor();
                boolean cuboidExactMatch = cubeDesc.getAllCuboids().contains(rec.getGtRequest().getCuboid().getId());
                if (!cuboidExactMatch) {
                    cuboidUnmatch.put(sql, new Pair<Long, Long>(rec.getGtRequest().getCuboid().getId(), cubeDesc
                            .getInitialCuboidScheduler().findBestMatchCuboid(rec.getGtRequest().getCuboid().getId())));
                }
            }
        }

        logger.info("=============================================");
        logger.info("There are {} failed queries.", failed.size());
        for (Map.Entry<String, QueryRecord> failEntry : failed.entrySet()) {
            logger.info("SQL: {}, Message: {}", failEntry.getKey().substring(0, failEntry.getKey().indexOf('\n')),
                    failEntry.getValue().getSqlResult().getMessage());
        }

        logger.info("There are {} queries whose cuboid not exactly matches.", cuboidUnmatch.size());
        for (Map.Entry<String, Pair<Long, Long>> unmatchEntry : cuboidUnmatch.entrySet()) {
            logger.info("SQL: {}, Expected Cuboid: {}, Actual Cuboid: {}",
                    unmatchEntry.getKey().substring(0, unmatchEntry.getKey().indexOf('\n')),
                    StringUtils.leftPad(Long.toBinaryString(unmatchEntry.getValue().getFirst()), 63, '0'),
                    StringUtils.leftPad(Long.toBinaryString(unmatchEntry.getValue().getSecond()), 63, '0'));
        }
    }
}
