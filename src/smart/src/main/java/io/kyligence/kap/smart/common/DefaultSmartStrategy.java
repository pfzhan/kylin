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

import org.apache.kylin.dimension.DictionaryDimEnc;

public class DefaultSmartStrategy implements ISmartStrategy {
    public static final ISmartStrategy INSTANCE = new DefaultSmartStrategy();
    public static final String NAME = "default";

    DefaultSmartStrategy() {
    }

    public int getRowkeyDictEncCardinalityMax() {
        return 1000000;
    }

    public int getRowkeyFixLenLengthMax() {
        return 1000;
    }

    public long getRowkeyUHCCardinalityMin() {
        return 1000000L;
    }

    public int getJointGroupCardinalityMax() {
        return 64;
    }

    public int getJointColNumMax() {
        return 5;
    }

    public double getDimDerivedRatio() {
        return 0.5;
    }

    public int getMandatoryCardinalityMax() {
        return 1;
    }

    public double getApproxEqualMax() {
        return 1.1D;
    }

    public double getApproxEqualMin() {
        return 0.9D;
    }

    public int getMandatoryEnableQueryMin() {
        return 15;
    }

    public int getRowkeyFilterPromotionTimes() {
        return 100000;
    }

    public double getApproxDiffMax() {
        return 0.01;
    }

    public String getRowkeyDefaultEnc() {
        return DictionaryDimEnc.ENCODING_NAME;
    }

    public double getPhyscalWeight() {
        return 1;
    }

    public double getBusinessWeight() {
        return 1;
    }

    public boolean getDomainQueryEnabled() {
        return false;
    }

    public boolean getMeasureQueryEnabled() {
        return true;
    }

    public boolean getAggGroupKeepLegacy() {
        return true;
    }

    public boolean getAggGroupStrictEnabled() {
        return true;
    }

    public int getAggGroupStrictRetryMax() {
        return 63;
    }

    public String getAggGroupStrategy() {
        return "default";
    }

    public int getDerivedStrictRetryMax() {
        return 1;
    }

    public boolean getCuboidCombinationOverride() {
        return false;
    }

    public boolean enableDimCapForAggGroupStrict() {
        return true;
    }

    public boolean enableJointForAggGroupStrict() {
        return false;
    }

    public int getDimCapMin() {
        return 1;
    }

    public String getModelScopeStrategy() {
        return "query";
    }

    public int getQueryDryRunThreads() {
        return 12;
    }

    public String getMeasureCountDistinctType() {
        return "hllc(10)";
    }

    public boolean enableModelInnerJoinExactlyMatch() {
        return true;
    }

    @Override
    public int getProposeRetryMax() {
        return 3;
    }
}