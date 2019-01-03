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

import io.kyligence.kap.cube.model.IndexPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.IndexEntity;
import io.kyligence.kap.cube.model.IndexEntity.IndexIdentifier;
import io.kyligence.kap.cube.model.LayoutEntity;
import io.kyligence.kap.smart.NSmartContext.NModelContext;

class NCuboidReducer extends NAbstractCubeProposer {

    private static final Logger logger = LoggerFactory.getLogger(NCuboidReducer.class);

    NCuboidReducer(NModelContext context) {
        super(context);
    }

    @Override
    public IndexPlan doPropose(IndexPlan indexPlan) {

        // get to be removed cuboids
        final Map<IndexIdentifier, IndexEntity> proposedCuboids = Maps.newLinkedHashMap();

        final CuboidSuggester cuboidSuggester = new CuboidSuggester(context, indexPlan, proposedCuboids);
        cuboidSuggester.suggestCuboids(context.getModelTree());

        // log before shrink cuboids
        proposedCuboids.forEach((cuboidIdentifier, indexEntity) -> {
            logger.debug("layouts after reduce:");
            indexEntity.getLayouts().forEach(layout -> logger.debug("{}", layout.getId()));
        });

        // remove cuboids
        Map<IndexIdentifier, List<LayoutEntity>> cuboidLayoutMap = Maps.newHashMap();

        proposedCuboids.forEach((identifier, cuboid) -> cuboidLayoutMap.put(identifier, cuboid.getLayouts()));

        indexPlan.removeLayouts(cuboidLayoutMap, this::hasExternalRef, LayoutEntity::equals, true, false);

        // log after shrink cuboids
        cuboidLayoutMap.forEach((cuboidIdentifier, nCuboidLayouts) -> {
            logger.debug("layouts after reduce:");
            nCuboidLayouts.forEach(layout -> logger.debug("{}", layout.getId()));
        });

        return indexPlan;
    }

    private boolean hasExternalRef(LayoutEntity layout) {
        // TODO the mapping of sqlPattern to layout should get from favorite query
        return false;
    }
}
