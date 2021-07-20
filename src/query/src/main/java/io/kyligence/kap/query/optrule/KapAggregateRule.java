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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;

import io.kyligence.kap.query.relnode.KapAggregateRel;

import static org.apache.kylin.query.relnode.OLAPRel.CONVENTION;

/**
 */
public class KapAggregateRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new KapAggregateRule();

    public KapAggregateRule() {
        super(LogicalAggregate.class, Convention.NONE, CONVENTION, "KapAggregateRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalAggregate agg = (LogicalAggregate) rel;

        // AVG() will be transformed into SUM()/COUNT() by AggregateReduceFunctionsRule.
        // Here only let the transformed plan pass.
        if (containsAvg(agg)) {
            return null;
        }

        RelTraitSet traitSet = agg.getTraitSet().replace(CONVENTION).simplify();
        RelNode convertedInput = agg.getInput() instanceof HepRelVertex ? agg.getInput()
                : convert(agg.getInput(), CONVENTION);
        try {
            return new KapAggregateRel(
                    agg.getCluster(),
                    traitSet,
                    convertedInput,
                    agg.indicator,
                    agg.getGroupSet(),
                    agg.getGroupSets(),
                    agg.getAggCallList());
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create OLAPAggregateRel!", e);
        }
    }

    private boolean containsAvg(LogicalAggregate agg) {
        for (AggregateCall call : agg.getAggCallList()) {
            SqlAggFunction func = call.getAggregation();
            if (func instanceof SqlAvgAggFunction) {
                return true;
            }
        }
        return false;
    }

}