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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.metadata.model.FunctionDesc;

public class SmartConfig {
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

    private String getOptional(String name, String defaultValue) {
        String val = kapConfig.getSmartModelingConf(name);
        if (val == null) {
            return defaultValue;
        } else {
            return val;
        }
    }

    private long getOptional(String name, long defaultValue) {
        return Long.parseLong(getOptional(name, Long.toString(defaultValue)));
    }

    private int getOptional(String name, int defaultValue) {
        return Integer.parseInt(getOptional(name, Integer.toString(defaultValue)));
    }

    private boolean getOptional(String name, boolean defaultValue) {
        return Boolean.parseBoolean(getOptional(name, Boolean.toString(defaultValue)));
    }

    public int getRowkeyDictEncCardinalityMax() {
        return getOptional("rowkey.dict-encoding.max-cardinality", 1000000);
    }

    public int getRowkeyFixLenLengthMax() {
        return getOptional("rowkey.fixlen-encoding.max-length", 1000);
    }

    public long getRowkeyUHCCardinalityMin() {
        return getOptional("rowkey.uhc.min-cardinality", 1000000L);
    }

    public String getRowkeyDefaultEnc() {
        return getOptional("rowkey.default-encoding", DictionaryDimEnc.ENCODING_NAME);
    }

    public String getMeasureCountDistinctType() {
        return getOptional("measure.count-distinct.return-type", FunctionDesc.FUNC_COUNT_DISTINCT_BIT_MAP);
    }

    // =========== based on rules to propose computed columns with sqlNode ====================

    public boolean isAdviseComputedColumnOnSqlNodeEnabled() {
        return getOptional("cc-advise-on-sqlnode", true);
    }

    public String[] getFunctionsAppliedToCCRules() {
        return getOptionalStringArray("cc-propose-functions", new String[] { "TIMESTAMP_DIFF", "TIMESTAMP_ADD" });
    }

    public String[] getSpecialCCRulesOnSqlNode() {
        return getOptionalStringArray("cc-propose-rules", new String[] { "io.kyligence.kap.smart.model.rule.AggFunctionRule",
                "io.kyligence.kap.smart.model.rule.CaseWhenRule" });
    }

    private String[] getOptionalStringArray(String key, String[] values) {
        String value = getOptional(key, null);
        return StringUtils.isBlank(value) ? values : new String[0];
    }
}
