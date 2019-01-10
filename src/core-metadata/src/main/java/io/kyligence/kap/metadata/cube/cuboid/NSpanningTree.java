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

package io.kyligence.kap.metadata.cube.cuboid;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;

public abstract class NSpanningTree implements Serializable {
    final protected Map<IndexEntity, Collection<LayoutEntity>> cuboids;
    final protected String cacheKey;

    public NSpanningTree(Map<IndexEntity, Collection<LayoutEntity>> cuboids, String cacheKey) {
        this.cuboids = cuboids;
        this.cacheKey = cacheKey;
    }

    abstract public boolean isValid(int cuboidId);

    abstract public int getCuboidCount();

    abstract public Collection<IndexEntity> getRootIndexEntities();

    abstract public Collection<LayoutEntity> getLayouts(IndexEntity cuboidDesc);

    abstract public Set<Integer> retrieveAllMeasures(IndexEntity root);

    abstract public IndexEntity getRootIndexEntity(IndexEntity cuboidDesc);

    abstract public IndexEntity getIndexEntity(long cuboidId);

    abstract public LayoutEntity getCuboidLayout(long cuboidLayoutId);

    abstract public IndexEntity getParentIndexEntity(IndexEntity cuboid);

    abstract public Collection<IndexEntity> getSpanningIndexEntities(IndexEntity cuboid);

    abstract public Collection<IndexEntity> getAllIndexEntities();

    abstract public void acceptVisitor(ISpanningTreeVisitor matcher);

    public interface ISpanningTreeVisitor {
        boolean visit(IndexEntity cuboidDesc);

        NLayoutCandidate getBestLayoutCandidate();
    }

    public String getCuboidCacheKey() {
        return cacheKey;
    }

    public Map<IndexEntity, Collection<LayoutEntity>> getCuboids() {
        return cuboids;
    }

    private transient List<Collection<IndexEntity>> cuboidsByLayer;

    public List<Collection<IndexEntity>> getCuboidsByLayer() {
        if (cuboidsByLayer != null) {
            return cuboidsByLayer;
        }

        int totalNum = 0;
        cuboidsByLayer = Lists.newArrayList();
        cuboidsByLayer.add(getRootIndexEntities());
        Collection<IndexEntity> lastLayer = cuboidsByLayer.get(cuboidsByLayer.size() - 1);
        totalNum += lastLayer.size();
        while (!lastLayer.isEmpty()) {
            List<IndexEntity> newLayer = Lists.newArrayList();
            for (IndexEntity parent : lastLayer) {
                newLayer.addAll(getSpanningIndexEntities(parent));
            }
            if (newLayer.isEmpty()) {
                break;
            }
            cuboidsByLayer.add(newLayer);
            totalNum += newLayer.size();
            lastLayer = newLayer;
        }

        int size = getCuboidCount();
        Preconditions.checkState(totalNum == size, "total Num: " + totalNum + " actual size: " + size);
        return cuboidsByLayer;
    }

    public int getBuildLevel() {
        return getCuboidsByLayer().size() - 1;
    }
}
