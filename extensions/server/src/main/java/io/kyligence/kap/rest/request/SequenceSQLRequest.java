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

package io.kyligence.kap.rest.request;

import org.apache.kylin.rest.request.SQLRequest;

import io.kyligence.kap.rest.sequencesql.ResultOpt;
import io.kyligence.kap.rest.sequencesql.SequenceOpt;

public class SequenceSQLRequest extends SQLRequest {

    protected long sequenceID = -1;
    protected int stepID = -1;//default value indicates appending this sql at the end of the sequence

    protected SequenceOpt sequenceOpt = null;
    protected ResultOpt resultOpt = null;

    public long getSequenceID() {
        return sequenceID;
    }

    public void setSequenceID(long sequenceID) {
        this.sequenceID = sequenceID;
    }

    public SequenceOpt getSequenceOpt() {
        return sequenceOpt;
    }

    public void setSequenceOpt(SequenceOpt sequenceOpt) {
        this.sequenceOpt = sequenceOpt;
    }

    public int getStepID() {
        return stepID;
    }

    public void setStepID(int stepID) {
        this.stepID = stepID;
    }

    public ResultOpt getResultOpt() {
        return resultOpt;
    }

    public void setResultOpt(ResultOpt resultOpt) {
        this.resultOpt = resultOpt;
    }
}
