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

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Union;
import org.apache.kylin.query.relnode.OlapRel;
import org.apache.kylin.query.relnode.OlapUnionRel;

/**
 */
public class OLAPUnionRule extends ConverterRule {

    public static final OLAPUnionRule INSTANCE = new OLAPUnionRule();

    public OLAPUnionRule() {
        super(Union.class, Convention.NONE, OlapRel.CONVENTION, "OLAPUnionRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final Union union = (Union) rel;
        final RelTraitSet traitSet = union.getTraitSet().replace(OlapRel.CONVENTION);
        final List<RelNode> inputs = union.getInputs();
        return new OlapUnionRel(rel.getCluster(), traitSet, convertList(inputs, OlapRel.CONVENTION), union.all);
    }
}
