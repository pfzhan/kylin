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

package org.apache.spark.sql.common;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparderContext {
    static final ThreadLocal<Boolean> _isAsyncQuery = new ThreadLocal<>();
    static final ThreadLocal<String> _separator = new ThreadLocal<>();
    static final ThreadLocal<Dataset<Row>> _df = new ThreadLocal<>();
    static final ThreadLocal<AtomicReference<Boolean>> _queryref = new ThreadLocal<>();

    public static void setAsAsyncQuery() {
        _isAsyncQuery.set(true);
    }

    public static Boolean isAsyncQuery() {
        return _isAsyncQuery.get() == null ? false : _isAsyncQuery.get();
    }

    public static void setSeparator(String separator) {
        _separator.set(separator);
    }

    public static String getSeparator() {
        return _separator.get() == null ? "," : _separator.get();
    }

    public static Dataset<Row> getDF() {
        return _df.get();
    }

    public static void setDF(Dataset<Row> df) {
        _df.set(df);
    }

    public static void setResultRef(AtomicReference<Boolean> ref) {
        _queryref.set(ref);
    }

    public static AtomicReference<Boolean> getResultRef() {
        return _queryref.get();
    }

    public static void clean() {
        _isAsyncQuery.set(null);
        _separator.set(null);
        _df.set(null);
    }
}
