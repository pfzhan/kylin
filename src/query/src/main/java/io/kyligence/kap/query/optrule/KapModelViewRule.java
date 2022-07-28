/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.query.optrule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.kylin.query.relnode.KapModelViewRel;
import org.apache.kylin.query.relnode.KapRel;

import com.google.common.collect.Lists;

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
