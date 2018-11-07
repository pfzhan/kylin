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

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidDesc.NCuboidIdentifier;
import io.kyligence.kap.smart.NSmartContext.NModelContext;

class NCuboidRefresher extends NAbstractCubeProposer {

    private static final Logger logger = LoggerFactory.getLogger(NCuboidRefresher.class);

    private String draftVersion;

    NCuboidRefresher(NModelContext context) {
        super(context);
        draftVersion = context.getSmartContext().getDraftVersion();
    }

    @Override
    void doPropose(NCubePlan cubePlan) {

        Preconditions.checkNotNull(draftVersion);

        Map<NCuboidIdentifier, NCuboidDesc> originalCuboidsMap = cubePlan.getWhiteListCuboidsMap();

        originalCuboidsMap.values().forEach(cuboid -> {
            logger.debug("layouts before refresh:");
            cuboid.getLayouts().forEach(layout -> logger.debug("{}", layout.getId()));
        });

        // if original propose cuboids unmodified, delete them
        originalCuboidsMap.forEach(
                (key, cuboid) -> cuboid.getLayouts().removeIf(layout -> layout.matchDraftVersion(draftVersion)));

        // propose cuboid again
        final CuboidSuggester cuboidSuggester = new CuboidSuggester(context, cubePlan, originalCuboidsMap);
        cuboidSuggester.suggestCuboids(context.getModelTree());

        final Collection<NCuboidDesc> cuboids = originalCuboidsMap.values();

        cuboids.forEach(cuboid -> {
            logger.debug("layouts after refresh:");
            cuboid.getLayouts().forEach(layout -> logger.debug("{}", layout.getId()));
        });

        // publish all layouts
        cuboids.forEach(cuboid -> cuboid.getLayouts().forEach(layout -> {
            if (layout.matchDraftVersion(draftVersion)) {
                layout.publish();
            }
        }));

        cubePlan.setCuboids(Lists.newArrayList(cuboids));
    }
}
