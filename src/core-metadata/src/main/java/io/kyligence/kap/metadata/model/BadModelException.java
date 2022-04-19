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
package io.kyligence.kap.metadata.model;

import static org.apache.kylin.common.exception.CommonErrorCode.UNKNOWN_ERROR_CODE;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.code.ErrorCodeProducer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BadModelException extends KylinException {

    public enum CauseType {
        WRONG_POSITION_DUE_TO_NAME, // another model is using this cc name on a different alias table
        WRONG_POSITION_DUE_TO_EXPR, // another model is using this cc's expression on a different alias table
        SAME_NAME_DIFF_EXPR, // another model already has defined same cc name but different expr
        SAME_EXPR_DIFF_NAME, // another model already has defined same expr but different cc name
        SELF_CONFLICT_WITH_SAME_NAME, // cc conflicts with self's other cc
        SELF_CONFLICT_WITH_SAME_EXPRESSION, // cc conflicts with self's other cc
        LOOKUP_CC_NOT_REFERENCING_ITSELF // see io.kyligence.kap.metadata.model.KapModel.initComputedColumns()
    }

    @JsonProperty
    private CauseType causeType;
    @JsonProperty
    private String advise;
    @JsonProperty
    private String conflictingModel;
    @JsonProperty
    private String badCC;//tell caller which cc is bad

    public BadModelException(org.apache.kylin.common.exception.ErrorCodeSupplier errorCodeSupplier, String message, CauseType causeType, String advise,
                             String conflictingModel, String badCC) {
        super(errorCodeSupplier, message);
        this.causeType = causeType;
        this.advise = advise;
        this.conflictingModel = conflictingModel;
        this.badCC = badCC;
    }

    public BadModelException(ErrorCodeProducer nerrorCodeProducer, CauseType causeType, String advise,
                             String conflictingModel, String badCC, Object... args) {
        super(nerrorCodeProducer, args);
        this.causeType = causeType;
        this.advise = advise;
        this.conflictingModel = conflictingModel;
        this.badCC = badCC;
    }

    public BadModelException(String message, CauseType causeType, String advise, String conflictingModel,
            String badCC) {
        this(UNKNOWN_ERROR_CODE, message, causeType, advise, conflictingModel, badCC);
    }

    public CauseType getCauseType() {
        return causeType;
    }

    public String getAdvise() {
        return advise;
    }

    public String getConflictingModel() {
        return conflictingModel;
    }

    public String getBadCC() {
        return badCC;
    }
}
