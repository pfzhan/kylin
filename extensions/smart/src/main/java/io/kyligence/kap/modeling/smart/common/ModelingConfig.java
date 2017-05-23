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

package io.kyligence.kap.modeling.smart.common;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;

public class ModelingConfig {
    private KapConfig kapConfig;
    private IModelingStrategy strategy;

    public static ModelingConfig getInstanceFromEnv() {
        return new ModelingConfig(KapConfig.getInstanceFromEnv());
    }

    public static ModelingConfig wrap(KylinConfig kylinConfig) {
        return new ModelingConfig(KapConfig.wrap(kylinConfig));
    }

    private ModelingConfig(KapConfig kapConfig) {
        this.kapConfig = kapConfig;

        String strategyName = this.kapConfig.getSmartModelingStrategy();
        if (strategyName.equalsIgnoreCase("default")) {
            strategy = DefaultModelingStrategy.INSTANCE;
        } else {
            throw new RuntimeException("Unknown Strategy: " + strategyName);
        }
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

    private double getOptional(String name, double defaultValue) {
        return Double.parseDouble(getOptional(name, Double.toString(defaultValue)));
    }

    private int getOptional(String name, int defaultValue) {
        return Integer.parseInt(getOptional(name, Integer.toString(defaultValue)));
    }

    private boolean getOptional(String name, boolean defaultValue) {
        return Boolean.parseBoolean(getOptional(name, Boolean.toString(defaultValue)));
    }

    public int getRowkeyDictEncCardinalityMax() {
        return getOptional("rowkey.dict-encoding.max-cardinality", strategy.getRowkeyDictEncCardinalityMax());
    }

    public int getRowkeyFixLenLengthMax() {
        return getOptional("rowkey.fixlen-encoding.max-length", strategy.getRowkeyFixLenLengthMax());
    }

    public long getRowkeyUHCCardinalityMin() {
        return getOptional("rowkey.uhc.min-cardinality", strategy.getRowkeyUHCCardinalityMin());
    }

    public int getJointGroupCardinalityMax() {
        return getOptional("joint.max-group-cardinality", strategy.getJointGroupCardinalityMax());
    }

    public int getJointColNumMax() {
        return getOptional("joint.max-column-num", strategy.getJointColNumMax());
    }

    public double getDimDerivedRatio() {
        return getOptional("dim.derived.ratio", strategy.getDimDerivedRatio());
    }

    public int getMandatoryCardinalityMax() {
        return getOptional("mandatory.max-cardinality", strategy.getMandatoryCardinalityMax());
    }

    public double getApproxEqualMax() {
        return getOptional("approx.eq.max", strategy.getApproxEqualMax());
    }

    public double getApproxEqualMin() {
        return getOptional("approx.eq.min", strategy.getApproxEqualMin());
    }

    public int getMandatoryEnableQueryMin() {
        return getOptional("mandatory.query-enabled.min", strategy.getMandatoryEnableQueryMin());
    }

    public int getRowkeyFilterPromotionTimes() {
        return getOptional("rowkey.filter-promotion.times", strategy.getRowkeyFilterPromotionTimes());
    }

    public double getApproxDiffMax() {
        return getOptional("approx.diff.max", strategy.getApproxDiffMax());
    }

    public String getRowkeyDefaultEnc() {
        return getOptional("rowkey.default-encoding", strategy.getRowkeyDefaultEnc());
    }

    public double getPhyscalWeight() {
        return getOptional("physcal.weight", strategy.getPhyscalWeight());
    }

    public double getBusinessWeight() {
        return getOptional("business.weight", strategy.getBusinessWeight());
    }

    public boolean getDomainQueryEnabled() {
        return getOptional("domain.query-enabled", strategy.getDomainQueryEnabled());
    }

    public boolean getAggGroupKeepLegacy() {
        return getOptional("aggGroup.keep-legacy", strategy.getAggGroupKeepLegacy());
    }

    public boolean getAggGroupStrictEnabled() {
        return getOptional("aggGroup.strict-enabled", strategy.getAggGroupStrictEnabled());
    }

    public int getAggGroupStrictCombinationMax() {
        return getOptional("aggGroup.strict.combination-max", strategy.getAggGroupStrictCombinationMax());
    }

    public int getAggGroupStrictRetryMax() {
        return getOptional("aggGroup.strict.retry-max", strategy.getAggGroupStrictRetryMax());
    }

    public int getDerivedStrictRetryMax() {
        return getOptional("derived.strict.retry-max", strategy.getDerivedStrictRetryMax());
    }
}
