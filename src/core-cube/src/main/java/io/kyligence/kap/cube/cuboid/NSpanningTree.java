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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public abstract class NSpanningTree implements Serializable {
    @JsonProperty("cuboids")
    final protected Map<NCuboidDesc, Collection<NCuboidLayout>> cuboids;
    final protected String cacheKey;

    public NSpanningTree(Map<NCuboidDesc, Collection<NCuboidLayout>> cuboids, String cacheKey) {
        this.cuboids = cuboids;
        this.cacheKey = cacheKey;
    }

    abstract public boolean isValid(int cuboidId);

    abstract public int getCuboidCount();

    abstract public Collection<NCuboidDesc> getRootCuboidDescs();

    abstract public Collection<NCuboidLayout> getLayouts(NCuboidDesc cuboidDesc);

    abstract public Set<Integer> retrieveAllMeasures(NCuboidDesc root);

    abstract public NCuboidDesc getRootCuboidDesc(NCuboidDesc cuboidDesc);

    abstract public NCuboidDesc getCuboidDesc(long cuboidId);

    abstract public NCuboidLayout getCuboidLayout(long cuboidLayoutId);

    abstract public NCuboidDesc getParentCuboidDesc(NCuboidDesc cuboid);

    abstract public Collection<NCuboidDesc> getSpanningCuboidDescs(NCuboidDesc cuboid);

    abstract public Collection<NCuboidDesc> getAllCuboidDescs();

    abstract public void acceptVisitor(ISpanningTreeVisitor matcher);

    public interface ISpanningTreeVisitor {
        boolean visit(NCuboidDesc cuboidDesc);

        NLayoutCandidate getBestLayoutCandidate();
    }

    public String getCuboidCacheKey() {
        return cacheKey;
    }

    public Map<NCuboidDesc, Collection<NCuboidLayout>> getCuboids() {
        return cuboids;
    }

    private transient List<Collection<NCuboidDesc>> cuboidsByLayer;

    public List<Collection<NCuboidDesc>> getCuboidsByLayer() {
        if (cuboidsByLayer != null) {
            return cuboidsByLayer;
        }

        int totalNum = 0;
        cuboidsByLayer = Lists.newArrayList();
        cuboidsByLayer.add(getRootCuboidDescs());
        Collection<NCuboidDesc> lastLayer = cuboidsByLayer.get(cuboidsByLayer.size() - 1);
        totalNum += lastLayer.size();
        while (!lastLayer.isEmpty()) {
            List<NCuboidDesc> newLayer = Lists.newArrayList();
            for (NCuboidDesc parent : lastLayer) {
                newLayer.addAll(getSpanningCuboidDescs(parent));
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
