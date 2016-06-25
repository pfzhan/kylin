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

package io.kyligence.kap.rest.sequencesql.topology;

import java.util.List;

import com.google.common.collect.Lists;

import io.kyligence.kap.rest.sequencesql.ResultOpt;

public class SequenceOptNode extends SequenceNode {
    protected List<SequenceNode> parents = Lists.newArrayList();
    protected ResultOpt opt;
    protected int optID;

    public SequenceOptNode(List<SequenceNode> parents, ResultOpt opt, int optID) {
        this.parents = parents;
        this.opt = opt;
        this.optID = optID;
    }

    @Override
    public String getIdentifier() {
        return "opt_" + optID;
    }

    public List<SequenceNode> getParents() {
        return parents;
    }

    public ResultOpt getOpt() {
        return opt;
    }

    public void setOpt(ResultOpt opt) {
        this.opt = opt;
    }

    public int getOptID() {
        return optID;
    }
}
