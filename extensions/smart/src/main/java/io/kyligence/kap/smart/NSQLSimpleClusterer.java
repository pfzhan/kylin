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

package io.kyligence.kap.smart;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.model.GreedyModelTreesBuilder;
import io.kyligence.kap.smart.model.ModelTree;

public class NSQLSimpleClusterer {
    public List<NSmartContext.NModelContext> cluster(NSmartContext smartContext) {
        GreedyModelTreesBuilder builder = new GreedyModelTreesBuilder(smartContext.getKylinConfig(),
                smartContext.getProject());
        List<ModelTree> modelTrees = builder.build(Arrays.asList(smartContext.getSqls()),
                Lists.newArrayList(smartContext.getOlapContexts().values()), null);
        List<NSmartContext.NModelContext> result = Lists.newArrayListWithExpectedSize(modelTrees.size());
        for (ModelTree modelTree : modelTrees) {
            NSmartContext.NModelContext ctx = new NSmartContext.NModelContext(smartContext);
            ctx.setModelTree(modelTree);
            result.add(ctx);
        }
        return result;
    }
}
