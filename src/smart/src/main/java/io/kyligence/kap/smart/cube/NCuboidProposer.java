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

package io.kyligence.kap.smart.cube;

import java.util.Map;

import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidDesc.NCuboidIdentifier;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.model.ModelTree;

public class NCuboidProposer extends NAbstractCubeProposer {

    NCuboidProposer(NSmartContext.NModelContext context) {
        super(context);
    }

    @Override
    void doPropose(NCubePlan cubePlan) {
        Map<NCuboidIdentifier, NCuboidDesc> cuboidDescMap = Maps.newLinkedHashMap();
        for (NCuboidDesc cuboidDesc : cubePlan.getAllCuboids()) {
            NCuboidIdentifier identifier = cuboidDesc.createCuboidIdentifier();
            if (!cuboidDescMap.containsKey(identifier)) {
                cuboidDescMap.put(identifier, cuboidDesc);
            } else {
                cuboidDescMap.get(identifier).getLayouts().addAll(cuboidDesc.getLayouts());
            }
        }

        NDataModel model = context.getTargetModel();
        ModelTree modelTree = context.getModelTree();
        CuboidSuggester suggester = new CuboidSuggester(context.getSmartContext(), model, cubePlan, cuboidDescMap);
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            Map<String, String> aliasMap = RealizationChooser.matches(model, ctx);
            ctx.fixModel(model, aliasMap);
            suggester.ingest(ctx, model);
            ctx.unfixModel();
        }

        cubePlan.setCuboids(Lists.newArrayList(cuboidDescMap.values()));
    }
}