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

interface ISmartStrategy {
    int getRowkeyDictEncCardinalityMax();

    int getRowkeyFixLenLengthMax();

    long getRowkeyUHCCardinalityMin();

    int getJointGroupCardinalityMax();

    int getJointColNumMax();

    double getDimDerivedRatio();

    int getMandatoryCardinalityMax();

    double getApproxEqualMax();

    double getApproxEqualMin();

    int getMandatoryEnableQueryMin();

    int getRowkeyFilterPromotionTimes();

    double getApproxDiffMax();

    String getRowkeyDefaultEnc();

    double getPhyscalWeight();

    double getBusinessWeight();

    boolean getDomainQueryEnabled();

    boolean getMeasureQueryEnabled();

    boolean getAggGroupKeepLegacy();

    boolean getAggGroupStrictEnabled();

    int getAggGroupStrictRetryMax();

    String getAggGroupStrategy();

    int getDerivedStrictRetryMax();

    boolean getCuboidCombinationOverride();

    boolean enableDimCapForAggGroupStrict();

    boolean enableJointForAggGroupStrict();

    int getDimCapMin();

    String getModelScopeStrategy();

    int getQueryDryRunThreads();

    String getMeasureCountDistinctType();

    boolean enableModelInnerJoinExactlyMatch();
}
