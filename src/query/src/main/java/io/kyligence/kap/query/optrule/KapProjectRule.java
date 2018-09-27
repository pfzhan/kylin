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

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.google.common.base.Supplier;

import io.kyligence.kap.query.relnode.KapProjectRel;
import io.kyligence.kap.query.relnode.KapRel;

/**
 */
public class KapProjectRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new KapProjectRule();

    public KapProjectRule() {
        super(LogicalProject.class, RelOptUtil.PROJECT_PREDICATE, Convention.NONE, KapRel.CONVENTION, "KapProjectRule");
    }

    @Override
    public RelNode convert(final RelNode rel) {

        final LogicalProject project = (LogicalProject) rel;
        final RelNode convert = convert(project.getInput(),
                project.getInput().getTraitSet().replace(KapRel.CONVENTION));
        final RelOptCluster cluster = convert.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSet().replace(KapRel.CONVENTION)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.project(mq, convert, project.getProjects());
                    }
                });
        KapProjectRel kapProjectRel = new KapProjectRel(convert.getCluster(), traitSet, convert, project.getProjects(),
                project.getRowType());
        return kapProjectRel;
    }
}
