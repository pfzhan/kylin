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

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.IndexEntity;
import io.kyligence.kap.cube.model.LayoutEntity;

public class NIndexTreeVisitor implements NSpanningTree.ISpanningTreeVisitor {
    public static NIndexTreeVisitor create(IndexEntity target) {
        return new NIndexTreeVisitor(target);
    }

    private final IndexEntity target;
    private final Comparator<NLayoutCandidate> comparator;
    private final SortedSet<NLayoutCandidate> results;

    private NIndexTreeVisitor(IndexEntity target) {
        this.target = target;

        comparator = NLayoutCandidateComparators.simple(); //TODO: use scored() to leverage cuboid stats
        results = Sets.newTreeSet(comparator);
    }

    @Override
    public boolean visit(IndexEntity indexEntity) {
        if (target.fullyDerive(indexEntity)) {
            results.addAll(
                    Collections2.transform(indexEntity.getLayouts(), new Function<LayoutEntity, NLayoutCandidate>() {
                        @Override
                        public NLayoutCandidate apply(@Nullable LayoutEntity input) {
                            Preconditions.checkNotNull(input);
                            return new NLayoutCandidate(input);
                        }
                    }));
            return true;
        }

        return false;
    }

    @Override
    public NLayoutCandidate getBestLayoutCandidate() {
        return results.first();
    }
}
