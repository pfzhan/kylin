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

package io.kyligence.kap.storage.parquet.adhoc.udf;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.hadoop.hive.ql.exec.UDF;

public class Truncate extends UDF {

    public int evaluate(int v, int x) {
        int remainder = v % x;
        if (remainder < 0) {
            remainder += x;
        }
        return v - remainder;
    }

    public long evaluate(long v, long x) {
        long remainder = v % x;
        if (remainder < 0) {
            remainder += x;
        }
        return v - remainder;
    }

    public String evaluate(String s, int maxLength) {
        if (s == null) {
            return null;
        } else if (s.length() > maxLength) {
            return s.substring(0, maxLength);
        } else {
            return s;
        }
    }

    public ByteString evaluate(ByteString s, int maxLength) {
        if (s == null) {
            return null;
        } else if (s.length() > maxLength) {
            return s.substring(0, maxLength);
        } else {
            return s;
        }
    }
}
