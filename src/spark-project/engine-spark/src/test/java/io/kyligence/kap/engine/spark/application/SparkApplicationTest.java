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

package io.kyligence.kap.engine.spark.application;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;
import io.kyligence.kap.engine.spark.job.ParamsConstants;
import io.kyligence.kap.engine.spark.job.RestfulJobProgressReport;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class SparkApplicationTest extends NLocalWithSparkSessionTest {

    File tempDir = new File("./temp/");
    File file1 = new File(tempDir, "temp1_" + ResourceDetectUtils.fileName());
    File file2 = new File(tempDir, "temp2_" + ResourceDetectUtils.fileName());

    @Before
    public void before() throws IOException {
        FileUtils.forceMkdir(tempDir);

    }

    @After
    public void after() {
        FileUtils.deleteQuietly(tempDir);
    }

    @Test
    public void testChooseContentSize() throws Exception {
        SparkApplication application = new SparkApplication() {
            @Override
            protected void doExecute() throws Exception {
                System.out.println("empty");
            }
        };

        // write resource_path file
        Map<String, Long> map1 = Maps.newHashMap();
        map1.put("1", 300L);
        ResourceDetectUtils.write(new Path(file1.getAbsolutePath()), map1);

        Map<String, Long> map2 = Maps.newHashMap();
        map2.put("1", 200L);
        ResourceDetectUtils.write(new Path(file2.getAbsolutePath()), map2);

        Assert.assertEquals("300b", application.chooseContentSize(new Path(tempDir.getAbsolutePath())));
    }

    @Test
    public void testUpdateSparkJobExtraInfo() throws Exception {
        overwriteSystemProp("spark.driver.param.taskId", "cb91189b-2b12-4527-aa35-0130e7d54ec0_01");

        RestfulJobProgressReport report = Mockito.spy(new RestfulJobProgressReport());

        SparkApplication application = Mockito.spy(new SparkApplication() {
            @Override
            protected void doExecute() throws Exception {
                System.out.println("empty");
            }
        });

        Mockito.doReturn("http://sandbox.hortonworks.com:8088/proxy/application_1561370224051_0160/").when(application)
                .getTrackingUrl(null, ss);

        Map<String, String> payload = new HashMap<>(5);
        payload.put("project", "test_job_output");
        payload.put("job_id", "cb91189b-2b12-4527-aa35-0130e7d54ec0");
        payload.put("task_id", "cb91189b-2b12-4527-aa35-0130e7d54ec0_01");
        payload.put("yarn_app_id", "application_1561370224051_0160");
        payload.put("yarn_app_url", "http://sandbox.hortonworks.com:8088/proxy/application_1561370224051_0160/");

        Map<String, String> extraInfo = new HashMap<>();
        extraInfo.put("yarn_app_id", "application_1561370224051_0160");
        extraInfo.put("yarn_app_url", "http://sandbox.hortonworks.com:8088/proxy/application_1561370224051_0160/");

        String payloadJson = JsonUtil.writeValueAsString(payload);
        Map<String, String> params = new HashMap<>();
        params.put(ParamsConstants.TIME_OUT, String.valueOf(getTestConfig().getUpdateJobInfoTimeout()));
        params.put(ParamsConstants.JOB_TMP_DIR, getTestConfig().getJobTmpDir("test_job_output", true));
        Mockito.doReturn(Boolean.TRUE).when(report).updateSparkJobInfo(params, "/kylin/api/jobs/spark", payloadJson);

        Assert.assertTrue(report.updateSparkJobExtraInfo(params, "/kylin/api/jobs/spark", "test_job_output",
                "cb91189b-2b12-4527-aa35-0130e7d54ec0", extraInfo));

        Mockito.verify(report).updateSparkJobInfo(params, "/kylin/api/jobs/spark", payloadJson);

        Mockito.reset(application);
        Mockito.reset(report);
        Mockito.doReturn("http://sandbox.hortonworks.com:8088/proxy/application_1561370224051_0160/").when(application)
                .getTrackingUrl(null, ss);
        Mockito.doReturn(Boolean.FALSE).when(report).updateSparkJobInfo(params,
                "/kylin/api/jobs/spark", payloadJson);
        Assert.assertFalse(report.updateSparkJobExtraInfo(params, "/kylin/api/jobs/spark", "test_job_output",
                "cb91189b-2b12-4527-aa35-0130e7d54ec0", extraInfo));

        Mockito.verify(report, Mockito.times(3)).updateSparkJobInfo(params, "/kylin/api/jobs/spark", payloadJson);
    }

    @Test
    public void testCheckRangePartitionTableIsExist() throws Exception {
        KylinBuildEnv.getOrCreate(getTestConfig());
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "tdh");
        TableDesc fact = tableMgr.getTableDesc("TDH_TEST.LINEORDER_PARTITION");
        fact.setTransactional(true);

        PartitionDesc partitionDesc = new PartitionDesc();
        ColumnDesc columnDesc = new ColumnDesc();
        columnDesc.setName("LO_DATE");
        columnDesc.setDatatype("date");
        columnDesc.setTable(fact);
        NDataModel nDataModel = new NDataModel();
        nDataModel.setUuid(UUID.randomUUID().toString());
        SegmentRange.TimePartitionedSegmentRange timePartitionedSegmentRange = new SegmentRange.TimePartitionedSegmentRange();
        timePartitionedSegmentRange.setStart(1637387522L);
        timePartitionedSegmentRange.setEnd(1637905922L);
        // fact.setSegmentRange(timePartitionedSegmentRange);
        TableRef tableRef = new TableRef(nDataModel, "LINEORDER_PARTITION", fact, false);
        partitionDesc.setPartitionDateColumnRef(new TblColRef(tableRef, columnDesc));
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd hh:mm:ss");
        fact.setPartitionDesc(partitionDesc);

        Set<TableRef> tableRefs = Sets.newHashSet();

        SparkApplication sparkApplication = Mockito.mock(SparkApplication.class);
        Mockito.when(sparkApplication.checkRangePartitionTableIsExist(Mockito.any())).thenCallRealMethod();
        tableRefs.add(tableRef);
        nDataModel.setAllTableRefs(tableRefs);
        Assert.assertFalse(sparkApplication.checkRangePartitionTableIsExist(nDataModel));

        NDataModel nDataModel2 = new NDataModel();
        nDataModel2.setUuid(UUID.randomUUID().toString());
        timePartitionedSegmentRange.setStart(1637387522L);
        timePartitionedSegmentRange.setEnd(1637905922L);
        // fact.setSegmentRange(timePartitionedSegmentRange);
        fact.setRangePartition(Boolean.TRUE);
        tableRef = new TableRef(nDataModel2, "LINEORDER_PARTITION", fact, false);
        partitionDesc.setPartitionDateColumnRef(new TblColRef(tableRef, columnDesc));
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd hh:mm:ss");
        fact.setPartitionDesc(partitionDesc);

        tableRefs.clear();
        tableRefs.add(tableRef);
        nDataModel2.setAllTableRefs(tableRefs);
        Assert.assertTrue(sparkApplication.checkRangePartitionTableIsExist(nDataModel2));
    }

}
