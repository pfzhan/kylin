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

import java.util.BitSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
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
        // get current cuboids
        Map<Pair<BitSet, BitSet>, NCuboidDesc> originalCuboids = Maps.newLinkedHashMap();
        for (NCuboidDesc cuboidDesc : cubePlan.getCuboids()) {
            BitSet dimBitSet = ImmutableBitSet.valueOf(cuboidDesc.getDimensions()).mutable();
            if (cuboidDesc.isTableIndex()) {
                // FIXME use better table index flag
                int tableIndexFlag = Integer.MAX_VALUE;
                dimBitSet.set(tableIndexFlag);
            }
            Pair<BitSet, BitSet> key = new Pair<>(dimBitSet,
                    ImmutableBitSet.valueOf(cuboidDesc.getMeasures()).mutable());
            NCuboidDesc desc = originalCuboids.get(key);

            if (desc == null) {
                originalCuboids.put(key, cuboidDesc);
            } else {
                desc.getLayouts().addAll(cuboidDesc.getLayouts());
            }
        }

        // get to be removed cuboids
        Map<Pair<BitSet, BitSet>, NCuboidDesc> proposedCuboids = Maps.newLinkedHashMap();
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
        for (Entry<Pair<BitSet, BitSet>, NCuboidDesc> cuboidPair : proposedCuboids.entrySet()) {
            Pair<BitSet, BitSet> cuboidKey = cuboidPair.getKey();
            NCuboidDesc cuboid = cuboidPair.getValue();
            NCuboidDesc originalCuboid = originalCuboids.get(cuboidKey);
            if (originalCuboid == null) {
                continue;
            }
            for (NCuboidLayout cuboidLayout : cuboid.getLayouts()) {
                // TODO check if this layout is used by other query
                boolean hasExternalRef = false;
                if (hasExternalRef) {
                    continue;
                }
                NCuboidLayout toRemoveLayout = null;
                for (NCuboidLayout originalLayout : originalCuboid.getLayouts()) {
                    if (CuboidSuggester.compareLayouts(originalLayout, cuboidLayout)) {
                        toRemoveLayout = originalLayout;
                        break;
                    }
                }
                if (toRemoveLayout == null) {
                    continue;
                }
                originalCuboid.getLayouts().remove(toRemoveLayout);
            }
            if (originalCuboid.getLayouts().isEmpty()) {
                originalCuboids.remove(cuboidKey);
            }
        }

        cubePlan.setCuboids(Lists.newArrayList(originalCuboids.values()));
    }
}
