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

package io.kyligence.kap.cube.cuboid;

import java.util.Comparator;
import java.util.SortedSet;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;

public class NCuboidDescTreeVisitor implements NSpanningTree.ICuboidTreeVisitor {
    public static NCuboidDescTreeVisitor create(NCuboidDesc target) {
        return new NCuboidDescTreeVisitor(target);
    }

    private final NCuboidDesc target;
    private final Comparator<NCuboidLayout> comparator;
    private final SortedSet<NCuboidLayout> results;

    private NCuboidDescTreeVisitor(NCuboidDesc target) {
        this.target = target;

        comparator = NCuboidLayoutComparators.simple(); //TODO: use scored() to leverage cuboid stats
        results = Sets.newTreeSet(comparator);
    }

    @Override
    public boolean visit(NCuboidDesc cuboidDesc) {
        if (target.fullyDerive(cuboidDesc)) {
            results.addAll(cuboidDesc.getLayouts());
            return true;
        }

        return false;
    }

    @Override
    public NCuboidLayout getMatched() {
        return results.first();
    }
}
