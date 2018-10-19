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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidDesc.NCuboidIdentifier;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.model.ModelTree;

public class NCuboidReducer extends NAbstractCubeProposer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NCuboidReducer.class);

    NCuboidReducer(NModelContext context) {
        super(context);
    }

    @Override
    void doPropose(NCubePlan cubePlan) {
        // get to be removed cuboids
        Map<NCuboidIdentifier, NCuboidDesc> proposedCuboids = Maps.newLinkedHashMap();
        NDataModel model = context.getTargetModel();
        ModelTree modelTree = context.getModelTree();
        CuboidSuggester suggester = new CuboidSuggester(context.getSmartContext(), model, cubePlan, proposedCuboids);
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            Map<String, String> aliasMap = RealizationChooser.matches(model, ctx);
            ctx.fixModel(model, aliasMap);
            try {
                suggester.ingest(ctx, model);
            } catch (Exception e) {
                LOGGER.error("Unable to suggest cuboid for CubePlan", e);
                continue;
            }
            ctx.unfixModel();
        }

        // remove cuboids
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(context.getSmartContext().getKylinConfig(),
                cubePlan.getProject());
        Map<NCuboidIdentifier, List<NCuboidLayout>> cuboidLayoutMap = Maps.newHashMap();
        for (Entry<NCuboidIdentifier, NCuboidDesc> entry : proposedCuboids.entrySet()) {
            cuboidLayoutMap.put(entry.getKey(), entry.getValue().getLayouts());
        }
        cubePlanManager.removeLayouts(cubePlan, cuboidLayoutMap, new Predicate<NCuboidLayout>() {
            @Override
            public boolean apply(@Nullable NCuboidLayout input) {
                // TODO check if this layout is used by other query
                boolean hasExternalRef = false;
                return hasExternalRef;
            }
        }, new Predicate2<NCuboidLayout, NCuboidLayout>() {
            @Override
            public boolean apply(NCuboidLayout o1, NCuboidLayout o2) {
                return CuboidSuggester.compareLayouts(o1, o2);
            }
        });
    }
}
