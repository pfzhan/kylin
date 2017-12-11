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

package org.apache.spark.sql.udf;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Truncate extends UDF {

    public Integer evaluate(Integer b0, Integer b1) {
        if (b0 == null || b1 == null) {
            return b0;
        }
        return evaluate(BigDecimal.valueOf(b0), b1).intValue();
    }

    public Long evaluate(Long b0, Integer b1) {
        if (b0 == null || b1 == null) {
            return b0;
        }
        return evaluate(BigDecimal.valueOf(b0), b1).longValue();
    }

    public Double evaluate(Double b0, Integer b1) {
        if (b0 == null || b1 == null) {
            return b0;
        }
        return evaluate(BigDecimal.valueOf(b0), b1).doubleValue();
    }

    public BigDecimal evaluate(BigDecimal b0, Integer b1) {
        if (b0 == null || b1 == null) {
            return b0;
        }
        return b0.movePointRight(b1).setScale(0, RoundingMode.DOWN).movePointLeft(b1);
    }
}
