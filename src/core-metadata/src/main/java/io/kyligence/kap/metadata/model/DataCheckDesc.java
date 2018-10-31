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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.io.Serializable;

@Getter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataCheckDesc implements Serializable {

    @JsonProperty("check_options")
    private long checkOptions;

    @JsonProperty("fault_threshold")
    private long faultThreshold;

    @JsonProperty("fault_actions")
    private long faultActions;

    public static DataCheckDesc valueOf(long checkOptions, long faultThreshold, long faultActions) {
        DataCheckDesc instance = new DataCheckDesc();
        instance.checkOptions = checkOptions;
        instance.faultThreshold = faultThreshold;
        instance.faultActions = faultActions;
        return instance;
    }

    public boolean checkDuplicatePK() {
        return CheckOptions.PK_DUPLICATE.match(checkOptions);
    }

    public boolean checkDataSkew() {
        return CheckOptions.DATA_SKEW.match(checkOptions);
    }

    public boolean checkNullOrBlank() {
        return CheckOptions.NULL_OR_BLANK_VALUE.match(checkOptions);
    }

    public boolean checkForceAnalysisLookup() {
        return CheckOptions.FORCE_ANALYSIS_LOOKUP.match(checkOptions);
    }

    public boolean isContinue(int count) {
        return count > faultThreshold && ActionOptions.CONTINUE.match(faultActions);
    }

    public boolean isFailed(int count) {
        return count > faultThreshold && ActionOptions.FAILED.match(faultActions);
    }

    enum CheckOptions {

        PK_DUPLICATE(1), DATA_SKEW(1 << 1), NULL_OR_BLANK_VALUE(1 << 2), FORCE_ANALYSIS_LOOKUP(1 << 3);

        private int value;

        CheckOptions(int value) {
            this.value = value;
        }

        boolean match(long checkOptions) {
            return (checkOptions & value) != 0;
        }
    }

    enum ActionOptions {

        FAILED(1), CONTINUE(1 << 1);

        private int value;

        ActionOptions(int value) {
            this.value = value;
        }

        boolean match(long actionOptions) {
            return (actionOptions & value) != 0;
        }
    }
}
