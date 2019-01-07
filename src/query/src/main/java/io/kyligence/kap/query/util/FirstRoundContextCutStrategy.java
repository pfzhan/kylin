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

package io.kyligence.kap.query.util;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;

import com.google.common.collect.Lists;

import io.kyligence.kap.query.relnode.ContextUtil;
import io.kyligence.kap.query.relnode.KapRel;

public class FirstRoundContextCutStrategy implements ICutContextStrategy {

    @Override
    public List<OLAPRel> cutOffContext(OLAPRel rootRel, RelNode parentOfRoot) {
        //Step 1.first round, cutting olap context
        KapRel.OLAPContextImplementor contextImplementor = new KapRel.OLAPContextImplementor();
        KapRel.ContextVisitorState initState = KapRel.ContextVisitorState.init();
        contextImplementor.visitChild(rootRel, rootRel, initState);
        if (initState.hasFreeTable()) {
            // if there are free tables, allocate a context for it
            contextImplementor.allocateContext((KapRel) rootRel, parentOfRoot);
        }
        toLeafJoinForm();

        ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER HEP PLANNER", rootRel);
        contextImplementor.optimizeContextCut();
        return Lists.newArrayList(rootRel);
    }

    @Override
    public boolean needCutOff(OLAPRel rootRel) {
        return true;
    }

    private void toLeafJoinForm() {
        // filter and project pull up
        for (OLAPContext context : ContextUtil.listContexts()) {
            RelNode parentOfTopNode = context.getParentOfTopNode();
            if (parentOfTopNode == null) {
                for (int i = 0; i < context.getTopNode().getInputs().size(); i++) {
                    context.getTopNode().replaceInput(i,
                            HepUtils.runRuleCollection(context.getTopNode().getInput(i), HepUtils.CUBOID_OPT_RULES));
                }
                ((KapRel) context.getTopNode()).setContext(context);
                return;
            }

            for (int i = 0; i < parentOfTopNode.getInputs().size(); i++) {
                if (context.getTopNode() != parentOfTopNode.getInput(i))
                    continue;

                RelNode newInput = HepUtils.runRuleCollection(parentOfTopNode.getInput(i), HepUtils.CUBOID_OPT_RULES);
                ((KapRel) newInput).setContext(context);
                context.setTopNode((KapRel) newInput);
                parentOfTopNode.replaceInput(i, newInput);
            }
        }
    }
}
