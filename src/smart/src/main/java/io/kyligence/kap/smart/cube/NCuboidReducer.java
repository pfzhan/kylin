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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidDesc.NCuboidIdentifier;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.smart.NSmartContext.NModelContext;

class NCuboidReducer extends NAbstractCubeProposer {

    private static final Logger logger = LoggerFactory.getLogger(NCuboidReducer.class);

    NCuboidReducer(NModelContext context) {
        super(context);
    }

    @Override
    void doPropose(NCubePlan cubePlan) {

        // get to be removed cuboids
        final Map<NCuboidIdentifier, NCuboidDesc> proposedCuboids = Maps.newLinkedHashMap();

        final CuboidSuggester cuboidSuggester = new CuboidSuggester(context, cubePlan, proposedCuboids);
        cuboidSuggester.suggestCuboids(context.getModelTree());

        // log before shrink cuboids
        proposedCuboids.forEach((cuboidIdentifier, cuboidDesc) -> {
            logger.debug("layouts after reduce:");
            cuboidDesc.getLayouts().forEach(layout -> logger.debug("{}", layout.getId()));
        });

        // remove cuboids
        Map<NCuboidIdentifier, List<NCuboidLayout>> cuboidLayoutMap = Maps.newHashMap();

        proposedCuboids.forEach((key, value) -> cuboidLayoutMap.put(key, value.getLayouts()));

        cubePlan.removeLayouts(cuboidLayoutMap, this::hasExternalRef, NCuboidLayout::equals);

        // log after shrink cuboids
        cuboidLayoutMap.forEach((cuboidIdentifier, nCuboidLayouts) -> {
            logger.debug("layouts after reduce:");
            nCuboidLayouts.forEach(layout -> logger.debug("{}", layout.getId()));
        });
    }

    private boolean hasExternalRef(NCuboidLayout layout) {
        // TODO the mapping of sqlPattern to layout should get from favorite query
        return false;
    }
}
