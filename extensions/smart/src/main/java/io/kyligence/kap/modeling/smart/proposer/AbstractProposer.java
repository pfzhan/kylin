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

package io.kyligence.kap.modeling.smart.proposer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.SelectRule;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.ArrayUtils;
import io.kyligence.kap.modeling.smart.ModelingContext;
import io.kyligence.kap.modeling.smart.common.ModelingConfig;
import io.kyligence.kap.query.mockup.Utils;

public abstract class AbstractProposer {
    final ModelingContext context;
    final ModelingConfig modelingConfig;

    AbstractProposer(ModelingContext context) {
        this.context = context;
        this.modelingConfig = context.getModelingConfig();
    }

    public CubeDesc propose(CubeDesc sourceCubeDesc) {
        CubeDesc workCubeDesc = CubeDesc.getCopyOf(sourceCubeDesc);
        preProcess(workCubeDesc);

        try {
            workCubeDesc.init(context.getKylinConfig());
        } catch (IllegalStateException e) {
            // if cube not tuned, then enlarge combination in override props to bypass init().
            Utils.setLargeCuboidCombinationConf(workCubeDesc.getOverrideKylinProps());
            workCubeDesc.init(context.getKylinConfig());
        }

        doPropose(workCubeDesc);
        return workCubeDesc;
    }

    abstract void doPropose(CubeDesc workCubeDesc);

    void preProcess(CubeDesc workCubeDesc) {
        // remove invalid agg groups before validate cube_desc
        Iterator<AggregationGroup> aggGroupItr = workCubeDesc.getAggregationGroups().iterator();
        while (aggGroupItr.hasNext()) {
            AggregationGroup aggGroup = aggGroupItr.next();
            if (aggGroup.getIncludes() == null || aggGroup.getIncludes().length == 0) {
                aggGroupItr.remove();
                continue;
            }

            // only keep valid joint and hierarchy groups, whose size > 1
            SelectRule selectRule = aggGroup.getSelectRule();
            List<List<String>> joints = Lists.newArrayList();
            List<List<String>> hiers = Lists.newArrayList();

            for (String[] joint_dim : selectRule.jointDims) {
                if (joint_dim != null && joint_dim.length > 1) {
                    List<String> jointOld = Arrays.asList(joint_dim);
                    joints.add(jointOld);
                }
            }
            for (String[] hierarchy_dim : selectRule.hierarchyDims) {
                if (hierarchy_dim != null && hierarchy_dim.length > 1) {
                    List<String> hierOld = Arrays.asList(hierarchy_dim);
                    hiers.add(hierOld);
                }
            }

            selectRule.jointDims = ArrayUtils.to2DArray(joints);
            selectRule.hierarchyDims = ArrayUtils.to2DArray(hiers);
        }
    }
}
