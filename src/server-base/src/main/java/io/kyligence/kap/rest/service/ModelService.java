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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kyligence.kap.cube.cuboid.NForestSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.ModifiedOrder;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author zy
 */
@Component("modelService")
public class ModelService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ModelService.class);

    public static final char[] VALID_MODELNAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();

    @Autowired
    private AclEvaluate aclEvaluate;

    public boolean isTableInModel(TableDesc table, String project) throws IOException {
        return getDataModelManager(project).getModelsUsingTable(table).size() > 0;
    }

    public List<String> getModelsUsingTable(TableDesc table, String project) throws IOException {
        return getDataModelManager(project).getModelsUsingTable(table);
    }

    public List<NDataModel> getModels(final String modelName, final String projectName, boolean exactMatch)
            throws IOException {

        aclEvaluate.checkIsGlobalAdmin();
        List<NDataModel> models = getDataModelManager(projectName).getDataModels();
        List<NDataModel> filterModels = new ArrayList<NDataModel>();
        for (NDataModel modelDesc : models) {
            boolean isModelMatch = StringUtils.isEmpty(modelName)
                    || (exactMatch && modelDesc.getName().toLowerCase().equals(modelName.toLowerCase()))
                    || (!exactMatch && modelDesc.getName().toLowerCase().contains(modelName.toLowerCase()));

            if (isModelMatch) {
                filterModels.add(modelDesc);
            }
        }

        Collections.sort(filterModels, new ModifiedOrder());

        return filterModels;
    }

    public Segments<NDataSegment> getSegments(String modelName, String project, long startTime, long endTime) {
        List<NCubePlan> cubePlans = getCubePlans(modelName, project);
        NDataflowManager dataflowManager = getDataflowManager(project);
        Segments<NDataSegment> segments = new Segments<NDataSegment>();
        final SegmentRange<Long> filterRange = new SegmentRange.TimePartitionedSegmentRange(startTime - 1, endTime + 1);
        for (NCubePlan cubeplan : cubePlans) {
            NDataflow dataflow = dataflowManager.getDataflow(cubeplan.getName());
            for (NDataSegment segment : dataflow.getSegments()) {
                if (segment.getSegRange().overlaps(filterRange))
                    segments.add(segment);
            }
        }

        return segments;
    }

    public List<NCuboidDesc> getAggIndexs(String modelName, String project) {
        List<NCuboidDesc> cuboidDescs = getCuboidDescs(modelName, project);
        List<NCuboidDesc> result = new ArrayList<NCuboidDesc>();
        for (NCuboidDesc cuboidDesc : cuboidDescs) {
            if (cuboidDesc.getId() < NCuboidDesc.TABLE_INDEX_START_ID)
                result.add(cuboidDesc);
        }
        return result;
    }

    public List<NCuboidDesc> getTableIndexs(String modelName, String project) {
        List<NCuboidDesc> cuboidDescs = getCuboidDescs(modelName, project);
        List<NCuboidDesc> result = new ArrayList<NCuboidDesc>();
        for (NCuboidDesc cuboidDesc : cuboidDescs) {
            if (cuboidDesc.getId() >= NCuboidDesc.TABLE_INDEX_START_ID)
                result.add(cuboidDesc);
        }
        return result;
    }

    public List<NCuboidDesc> getCuboidDescs(String modelName, String project) {
        List<NCubePlan> cubePlans = getCubePlans(modelName, project);
        List<NCuboidDesc> cuboidDescs = new ArrayList<NCuboidDesc>();
        for (NCubePlan cubeplan : cubePlans) {
            cuboidDescs.addAll(cubeplan.getCuboids());
        }
        return cuboidDescs;
    }

    public NCuboidDesc getCuboidById(String modelName, String project, Long cuboidId) {
        List<NCubePlan> cubePlans = getCubePlans(modelName, project);
        NCuboidDesc cuboidDesc = null;
        for (NCubePlan cubeplan : cubePlans) {
            if (null != cubeplan.getCuboidDesc(cuboidId)) {
                cuboidDesc = cubeplan.getCuboidDesc(cuboidId);
                break;
            }
        }

        return cuboidDesc;
    }

    public String getModelJson(String modelName, String project) throws JsonProcessingException {
        NDataModel modelDesc = getDataModelManager(project).getDataModelDesc(modelName);
        return JsonUtil.writeValueAsIndentString(modelDesc);
    }

    public List<NForestSpanningTree> getModelRelations(String modelName, String project) {
        List<NCubePlan> cubePlans = getCubePlans(modelName, project);
        List<NForestSpanningTree> result = new ArrayList<NForestSpanningTree>();
        for (NCubePlan cubeplan : cubePlans) {
            NSpanningTree spanningTree = cubeplan.getSpanningTree();
            NForestSpanningTree nForestSpanningTree = new NForestSpanningTree(spanningTree.getCuboids(),
                    spanningTree.getCuboidCacheKey());
            result.add(nForestSpanningTree);
        }
        return result;
    }

    public List<NDataModel> getRelateModels(String project, String table) throws IOException {
        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        NDataModelManager dataModelManager = getDataModelManager(project);
        List<String> models = dataModelManager.getModelsUsingTable(tableDesc);
        List<NDataModel> dataModels = new ArrayList<NDataModel>();
        for (String model : models) {
            dataModels.add(dataModelManager.getDataModelDesc(model));
        }
        return dataModels;
    }

    private List<NCubePlan> getCubePlans(String modelName, String project) {
        NCubePlanManager cubePlanManager = getCubePlanManager(project);
        List<NCubePlan> cubePlans = cubePlanManager.findMatchingCubePlan(modelName, project,
                KylinConfig.getInstanceFromEnv());
        return cubePlans;
    }
}
