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

package io.kyligence.kap.query.optrule;

import com.google.common.collect.Lists;
import io.kyligence.kap.query.relnode.KapModelViewRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import io.kyligence.kap.query.relnode.KapRel;

/**
 */
public class KapModelViewRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new KapModelViewRule();

    public KapModelViewRule() {
        super(KapModelViewRel.class, Convention.NONE, KapRel.CONVENTION, "KapModelViewRule");
    }

    @Override
    public RelNode convert(RelNode call) {
        KapModelViewRel modelViewRel = (KapModelViewRel) call;
        RelTraitSet origTraitSet = modelViewRel.getTraitSet();
        RelTraitSet traitSet = origTraitSet.replace(KapRel.CONVENTION).simplify();

        RelNode convertedInput = modelViewRel.getInput() instanceof HepRelVertex ? modelViewRel.getInput()
                : convert(modelViewRel.getInput(), KapRel.CONVENTION);
        return modelViewRel.copy(traitSet, Lists.newArrayList(convertedInput));
    }

}
