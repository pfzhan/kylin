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

package org.apache.kylin.source.adhocquery;

import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;

import java.util.LinkedList;
import java.util.List;

public class PushdownResult {
    private final Iterable<List<String>> rows;
    private final int size;
    private final List<SelectedColumnMeta> columnMetas;

    public PushdownResult(Iterable<List<String>> rows, int size, List<SelectedColumnMeta> columnMetas) {
        this.rows = rows;
        this.size = size;
        this.columnMetas = columnMetas;
    }

    public static PushdownResult emptyResult() {
        return new PushdownResult(new LinkedList<>(), 0, new LinkedList<>());
    }

    public Iterable<List<String>> getRows() {
        return rows;
    }

    public int getSize() {
        return size;
    }

    public List<SelectedColumnMeta> getColumnMetas() {
        return columnMetas;
    }
}
