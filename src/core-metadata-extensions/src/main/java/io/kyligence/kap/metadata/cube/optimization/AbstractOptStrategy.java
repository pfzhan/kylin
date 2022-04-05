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

package io.kyligence.kap.metadata.cube.optimization;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

public abstract class AbstractOptStrategy {

    @Getter
    @Setter(AccessLevel.PROTECTED)
    private GarbageLayoutType type;

    /**
     * Every subclass of AbstractStrategy should override this method.
     */
    protected abstract Set<Long> doCollect(List<LayoutEntity> inputLayouts, NDataflow dataflow, boolean needLog);

    public final Set<Long> collectGarbageLayouts(List<LayoutEntity> inputLayouts, NDataflow dataflow, boolean needLog) {
        List<LayoutEntity> toHandleLayouts = beforeCollect(inputLayouts);
        Set<Long> garbageSet = doCollect(toHandleLayouts, dataflow, needLog);
        afterCollect(inputLayouts, garbageSet);
        return garbageSet;
    }

    private List<LayoutEntity> beforeCollect(List<LayoutEntity> inputLayouts) {
        List<LayoutEntity> layoutsToHandle = Lists.newArrayList(inputLayouts);
        skipOptimizeTableIndex(layoutsToHandle);
        return layoutsToHandle;
    }

    protected abstract void skipOptimizeTableIndex(List<LayoutEntity> inputLayouts);

    private void afterCollect(List<LayoutEntity> inputLayouts, Set<Long> garbages) {
        inputLayouts.removeIf(layout -> garbages.contains(layout.getId()));
    }
}
