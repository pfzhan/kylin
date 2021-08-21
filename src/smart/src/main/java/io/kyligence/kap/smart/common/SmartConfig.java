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

package io.kyligence.kap.smart.common;

import java.io.Serializable;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;

import io.kyligence.kap.common.obf.IKeep;

public class SmartConfig implements Serializable, IKeep {
    private final KapConfig kapConfig;

    private SmartConfig(KapConfig kapConfig) {
        this.kapConfig = kapConfig;
    }

    public static SmartConfig getInstanceFromEnv() {
        return new SmartConfig(KapConfig.getInstanceFromEnv());
    }

    public static SmartConfig wrap(KylinConfig kylinConfig) {
        return new SmartConfig(KapConfig.wrap(kylinConfig));
    }

    public KylinConfig getKylinConfig() {
        return kapConfig.getKylinConfig();
    }

    private String getOptional(String name, String defaultValue) {
        String val = kapConfig.getSmartModelingConf(name);
        if (val == null) {
            return defaultValue;
        } else {
            return val.trim();
        }
    }

    private long getOptional(String name, long defaultValue) {
        return Long.parseLong(getOptional(name, Long.toString(defaultValue)));
    }

    private boolean getOptional(String name, boolean defaultValue) {
        return Boolean.parseBoolean(getOptional(name, Boolean.toString(defaultValue)));
    }

    public long getRowkeyUHCCardinalityMin() {
        return getOptional("rowkey.uhc.min-cardinality", 1000000L);
    }

    public String getMeasureCountDistinctType() {
        return getOptional("measure.count-distinct.return-type", FunctionDesc.FUNC_COUNT_DISTINCT_BIT_MAP);
    }

    public long getComputedColumnOnGroupKeySuggestionMinCardinality() {
        return getOptional("computed-column.suggestion.group-key.minimum-cardinality", 10000L);
    }

    public long getComputedColumnOnFilterKeySuggestionMinCardinality() {
        return getOptional("computed-column.suggestion.filter-key.minimum-cardinality", 10000L);
    }

    public Boolean enableComputedColumnOnFilterKeySuggestion() {
        return Boolean.parseBoolean(getOptional("computed-column.suggestion.filter-key.enabled", "FALSE"));
    }

    public boolean needProposeCcIfNoSampling() {
        return getOptional("computed-column.suggestion.enabled-if-no-sampling", false);
    }

    public boolean startMemoryTuning() {
        return getOptional("memory-tuning", true);
    }

    public String getModelOptRule() {
        return getOptional("model-opt-rule", "");
    }

    public boolean skipUselessMetadata() {
        return getOptional("skip-useless-metadata", true);
    }

    public String getProposeRunnerImpl() {
        return getOptional("propose-runner-type", "fork");
    }
}
