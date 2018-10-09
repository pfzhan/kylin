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

package org.apache.kylin.query.optrule;

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
import org.apache.kylin.query.relnode.OLAPProjectRel;
import org.apache.kylin.query.relnode.OLAPRel;

import com.google.common.base.Supplier;

/**
 */
public class OLAPProjectRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new OLAPProjectRule();

    public OLAPProjectRule() {
        super(LogicalProject.class, RelOptUtil.PROJECT_PREDICATE, Convention.NONE, OLAPRel.CONVENTION,
                "OLAPProjectRule");
    }

    @Override
    public RelNode convert(final RelNode rel) {

        //  KYLIN-3281
        //  OLAPProjectRule can't normal working with projectRel[input=sortRel]
        final LogicalProject project = (LogicalProject) rel;
        final RelNode convertChild = convert(project.getInput(),
                project.getInput().getTraitSet().replace(OLAPRel.CONVENTION));
        final RelOptCluster cluster = convertChild.getCluster();
        final RelTraitSet traitSet = cluster.traitSet().replace(OLAPRel.CONVENTION)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        //  CALCITE-88
                        return RelMdCollation.project(cluster.getMetadataQuery(), convertChild, project.getProjects());
                    }
                });
        return new OLAPProjectRel(convertChild.getCluster(), traitSet, convertChild, project.getProjects(),
                project.getRowType());
    }
}
