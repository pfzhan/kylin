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

package io.kyligence.kap.rest.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.engine.spark.utils.ComputedColumnEvalUtil;
import io.kyligence.kap.job.util.ExecutableUtils;
import io.kyligence.kap.junit.rule.TransactionExceptedException;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.util.ExpandableMeasureUtil;
import io.kyligence.kap.metadata.query.QueryTimesResponse;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.constant.ModelAttributeEnum;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.delegate.JobMetadataBaseDelegate;
import io.kyligence.kap.rest.delegate.JobMetadataContract;
import io.kyligence.kap.rest.delegate.JobMetadataInvoker;
import io.kyligence.kap.rest.delegate.JobMetadataRequest;
import io.kyligence.kap.rest.response.FusionModelResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.service.params.ModelQueryParams;
import io.kyligence.kap.rest.util.ModelTriple;
import io.kyligence.kap.streaming.jobs.StreamingJobListener;
import lombok.val;

public class ModelServiceQueryTest extends SourceTestCase {
    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private final ModelQueryService modelQueryService = Mockito.spy(new ModelQueryService());

    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @InjectMocks
    private final FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @InjectMocks
    private final TableService tableService = Mockito.spy(new TableService());

    @InjectMocks
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    private StreamingJobListener eventListener = new StreamingJobListener();

    private JobMetadataInvoker jobMetadataInvoker = Mockito.spy(new JobMetadataInvoker());

    protected String getProject() {
        return "default";
    }

    @Before
    public void setup() {
        super.setup();
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.model.multi-partition-enabled", "true");
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "expandableMeasureUtil",
                new ExpandableMeasureUtil((model, ccDesc) -> {
                    String ccExpression = KapQueryUtil.massageComputedColumn(model, model.getProject(), ccDesc,
                            AclPermissionUtil.prepareQueryContextACLInfo(model.getProject(),
                                    semanticService.getCurrentUserGroups()));
                    ccDesc.setInnerExpression(ccExpression);
                    ComputedColumnEvalUtil.evaluateExprAndType(model, ccDesc);
                }));
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);
        JobMetadataInvoker.setDelegate(new JobMetadataContractTest());
        ReflectionTestUtils.setField(modelService, "jobMetadataInvoker", jobMetadataInvoker);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "fusionModelService", fusionModelService);

        modelService.setSemanticUpdater(semanticService);
        modelService.setIndexPlanService(indexPlanService);
        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);

        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }
        EventBusFactory.getInstance().register(eventListener, true);
        EventBusFactory.getInstance().register(modelBrokenListener, false);

        ExecutableUtils.initJobFactory();
    }

    private class JobMetadataContractTest extends JobMetadataBaseDelegate implements JobMetadataContract {

        @Override
        public String addSecondStorageJob(JobMetadataRequest jobMetadataRequest) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void stopBatchJob(String project, TableDesc tableDesc) {
            throw new UnsupportedOperationException();
        }
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        EventBusFactory.getInstance().unregister(eventListener);
        EventBusFactory.getInstance().unregister(modelBrokenListener);
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testQueryModels() {
        String project = "streaming_test";
        val modelList = modelService.getModels(null, null, true, project, "ADMIN", Lists.newArrayList(), "", 0, 8,
                "last_modify", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true);
        Assert.assertEquals(11, modelList.getTotalSize());
        Assert.assertEquals(8, modelList.getValue().size());

        val modelList1 = modelService.getModels(null, null, true, project, "ADMIN", Lists.newArrayList(), "", 1, 10,
                "usage", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true);
        Assert.assertEquals(11, modelList1.getTotalSize());
        Assert.assertEquals(1, modelList1.getValue().size());

        val modelResponse = modelList1.getValue().get(0);
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);

        val triple = new ModelTriple(dfMgr.getDataflow(modelResponse.getUuid()), modelResponse);
        Assert.assertTrue(triple.getLeft() instanceof NDataflow);
        Assert.assertTrue(triple.getMiddle() instanceof NDataModel);
        Assert.assertNull(triple.getRight());

        val modelList2 = modelService.getModels(null, null, true, project, "ADMIN", Lists.newArrayList(), "", 1, 5,
                "storage", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true);
        Assert.assertEquals(11, modelList2.getTotalSize());
        Assert.assertEquals(5, modelList2.getValue().size());
        Assert.assertTrue(((NDataModelResponse) modelList2.getValue().get(0))
                .getStorage() >= ((NDataModelResponse) modelList2.getValue().get(4)).getStorage());

        val modelList3 = modelService.getModels(null, null, true, project, "ADMIN", Lists.newArrayList(), "", 1, 10,
                "expansionrate", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, false);
        Assert.assertEquals(1, modelList3.getValue().size());

        val modelList4 = modelService.getModels(null, null, true, project, "ADMIN", Lists.newArrayList(), "", 0, 10,
                "expansionrate", false, "ADMIN",
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID),
                974198646000L, System.currentTimeMillis(), true);
        Assert.assertEquals(10, modelList4.getValue().size());

        val modelList5 = modelService.getModels(null, null, true, project, "ADMIN", Lists.newArrayList(), "", 1, 6, "",
                true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, false);
        Assert.assertEquals(5, modelList5.getValue().size());
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        val modelList6 = modelService.getModels(null, null, true, project, "ADMIN", Lists.newArrayList(), "", 1, 6, "",
                true, null, Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING,
                        ModelAttributeEnum.HYBRID, ModelAttributeEnum.SECOND_STORAGE),
                null, null, false);
        Assert.assertEquals(5, modelList6.getValue().size());
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");

        // used for getModels without sortBy field
        val modelList7 = modelService.getModels(null, null, true, project, "ADMIN", Lists.newArrayList(), "", 0, 6, "",
                true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, false);
        Assert.assertEquals(6, modelList7.getValue().size());
    }

    @Test
    public void testConvertToDataModelResponseBroken() {
        List<ModelAttributeEnum> modelAttributeSet = Lists.newArrayList(ModelAttributeEnum.BATCH);
        ModelQueryParams modelQueryParams = new ModelQueryParams("", null, true, "default", null, null, 0, 10, "", true,
                null, modelAttributeSet, null, null, true);

        val tripleList = modelQueryService.getModels(modelQueryParams);
        ModelTriple modelTriple = tripleList.get(0);
        NDataModel dataModel = modelTriple.getDataModel();

        NDataModelResponse nDataModelResponse = modelService.convertToDataModelResponseBroken(dataModel);
        Assert.assertEquals(ModelStatusToDisplayEnum.BROKEN, nDataModelResponse.getStatus());
    }

    @Test
    public void testGetFusionModel() {
        String project = "streaming_test";
        String modelName = "streaming_test";
        List<NDataModelResponse> models = modelService.getModels(modelName, project, false, null, Lists.newArrayList(),
                null, false, null, null, null, true);

        FusionModelResponse model = (FusionModelResponse) models.get(0);
        Assert.assertEquals(0, model.getAvailableIndexesCount());
        Assert.assertEquals(3, model.getTotalIndexes());
        Assert.assertEquals(5, model.getStreamingIndexes());
        Assert.assertEquals(10, model.getUsage());
        Assert.assertEquals(0, model.getStorage());
        Assert.assertEquals(0, model.getSource());

        String modelName1 = "AUTO_MODEL_P_LINEORDER_1";
        NDataModelResponse model1 = modelService
                .getModels(modelName1, project, false, null, Lists.newArrayList(), null, false, null, null, null, true)
                .get(0);
        Assert.assertEquals(0, model1.getAvailableIndexesCount());
        Assert.assertEquals(1, model1.getTotalIndexes());
        Assert.assertEquals(0, model1.getStorage());
        Assert.assertEquals(0, model1.getSource());

        String modelName2 = "model_streaming";
        DataResult<List<NDataModel>> modelResult2 = modelService.getModels("4965c827-fbb4-4ea1-a744-3f341a3b030d",
                modelName2, true, project, "ADMIN", Lists.newArrayList(), "", 0, 10, "last_modify", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID,
                        ModelAttributeEnum.SECOND_STORAGE),
                null, null, true);
        List<NDataModel> models2 = modelResult2.getValue();
        FusionModelResponse model2 = (FusionModelResponse) models2.get(0);

        Assert.assertEquals(12010, model2.getOldParams().getInputRecordCnt());
        Assert.assertEquals(1505415, model2.getOldParams().getInputRecordSizeBytes());
        Assert.assertEquals(396, model2.getOldParams().getSizeKB());
    }

    @Test
    public void testGetRelatedModels() {
        List<RelatedModelResponse> models = modelService.getRelateModels("default", "EDW.TEST_CAL_DT", "");
        Assert.assertEquals(0, models.size());
        List<RelatedModelResponse> models2 = modelService.getRelateModels("default", "DEFAULT.TEST_KYLIN_FACT",
                "nmodel_basic_inner");
        Assert.assertEquals(1, models2.size());
        doReturn(new ArrayList<>()).when(modelService).addOldParams(anyString(), any());

        val models3 = modelService.getModels("741ca86a-1f13-46da-a59f-95fb68615e3a", null, true, "default", "ADMIN",
                Lists.newArrayList(), "DEFAULT.TEST_KYLIN_FACT", 0, 8, "last_modify", true, null, null, null, null,
                true);
        Assert.assertEquals(1, models3.getTotalSize());
    }
}
