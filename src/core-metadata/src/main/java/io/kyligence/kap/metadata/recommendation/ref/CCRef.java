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

package io.kyligence.kap.metadata.recommendation.ref;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class CCRef extends RecommendationRef {

    public CCRef(ComputedColumnDesc cc, int id) {
        this.setId(id);
        this.setEntity(cc);
        this.setName(cc.getFullName());
        this.setContent(cc.getExpression());
        this.setDataType(cc.getDatatype());
    }

    public ComputedColumnDesc getCc() {
        return (ComputedColumnDesc) getEntity();
    }

    @Override
    public void rebuild(String newName) {
        ComputedColumnDesc cc = getCc();
        cc.setColumnName(newName);
        this.setName(cc.getFullName());
    }

    public boolean isIdentical(CCRef anotherCC) {
        if (anotherCC == null) {
            return false;
        }
        ComputedColumnDesc thisCC = this.getCc();
        ComputedColumnDesc thatCC = anotherCC.getCc();

        return thisCC.getFullName().equalsIgnoreCase(thatCC.getFullName())
                && thisCC.getDatatype().equalsIgnoreCase(thatCC.getDatatype())
                && thisCC.getInnerExpression().equalsIgnoreCase(thatCC.getInnerExpression());
    }
}
