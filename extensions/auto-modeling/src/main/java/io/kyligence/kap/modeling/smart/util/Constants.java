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

package io.kyligence.kap.modeling.smart.util;

import org.apache.kylin.dimension.DictionaryDimEnc;

public class Constants {
    // thresholds
    public final static int DIM_ENCODING_DICT_CARDINALITY_MAX = 1000000;
    public final static int DIM_ENCODING_FIXLEN_LENGTH_MAX = Integer.MAX_VALUE;
    public final static int DIM_UHC_MIN = 1000000;
    public final static int DIM_JOINT_FORCE_CARDINALITY_GROUP_MAX = 64;
    public final static int DIM_JOINT_FORCE_CARDINALITY_COL_MAX = 20;
    public final static double DIM_DERIVED_PK_RATIO = 0.5; // 2~3
    public final static int DIM_MANDATORY_FORCE_CARDINALITY_MAX = 1;
    public final static double DIM_AGG_GROUP_APPROX_EQUAL_MAX = 1.1;
    public final static double DIM_AGG_GROUP_APPROX_EQUAL_MIN = 0.9;
    public final static int DIM_AGG_GROUP_JOINT_ELEMENTS_MAX = 5;
    public final static int DIM_AGG_GROUP_MANDATORY_QUERY_MIN = 15;
    public final static double DIM_ROWKEY_FILRER_PROMOTION_TIMES = 100000;
    public final static double DIM_AGG_GROUP_DIFF_MIN = 0.01;

    // values
    public final static String DIM_DEREIVED_COLUMN_NAME = "{FK}";
    public final static String DIM_ENCODING_DEFAULT = DictionaryDimEnc.ENCODING_NAME;
    public final static String DIM_DEREIVED_NAME_SUFFIX = "DERIVED";

    // coe
    public final static int COE_PHYSCAL = 1;
    public final static int COE_BUSINESS = 1;

    public final static boolean DOMAIN_USE_QUERY = false;
}
