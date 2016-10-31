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
