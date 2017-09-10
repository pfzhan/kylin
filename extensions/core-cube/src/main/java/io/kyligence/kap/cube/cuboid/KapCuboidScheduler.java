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

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.cuboid.DefaultCuboidScheduler;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;

import io.kyligence.kap.common.obf.IKeep;

public class KapCuboidScheduler extends CuboidScheduler implements IKeep {
    
    private final CuboidScheduler delegate;

    public KapCuboidScheduler(CubeDesc cubeDesc) {
        super(cubeDesc);
        
        if (KylinVersion.compare(cubeDesc.getVersion(), "2.1.0.20403") < 0)
            delegate = new DefaultCuboidScheduler(cubeDesc);
        else
            delegate = new KapCuboidScheduler243(cubeDesc);
    }

    public Set<Long> getAllCuboidIds() {
        return delegate.getAllCuboidIds();
    }

    public boolean isValid(long requestCuboid) {
        return delegate.isValid(requestCuboid);
    }
    
    public int getCuboidCount() {
        return delegate.getCuboidCount();
    }

    public List<Long> getSpanningCuboid(long parentCuboid) {
        return delegate.getSpanningCuboid(parentCuboid);
    }

    public long findBestMatchCuboid(long requestCuboid) {
        return delegate.findBestMatchCuboid(requestCuboid);
    }

    public Set<Long> calculateCuboidsForAggGroup(AggregationGroup agg) {
        return delegate.calculateCuboidsForAggGroup(agg);
    }

    public String getCuboidCacheKey() {
        return delegate.getCuboidCacheKey();
    }
    
}
