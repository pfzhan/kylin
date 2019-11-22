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

package org.apache.kylin.rest.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kylin.rest.util.PagingUtil;

import java.util.Collection;
import java.util.List;

@Data
@AllArgsConstructor
public class DataResult<T extends Collection> {

    private T value;

    @JsonProperty("total_size")
    private int totalSize = 0;

    private int offset = 0;

    private int limit = 0;

    public DataResult(T data, int totalSize) {
        this.value = data;
        this.totalSize = totalSize;
    }

    public static <T extends Collection> DataResult<T> get(T data, int totalSize) {
        return new DataResult<>(data, totalSize);
    }

    public static <T extends Collection> DataResult<T> get(T data, T allData) {
        return get(data, allData, 0, 0);
    }

    public static <T extends Collection> DataResult<T> get(T data, T allData, int offset, int limit) {
        if (null == allData) {
            return new DataResult<>(data, 0, offset, limit);
        }

        return new DataResult<>(data, allData.size(), offset, limit);
    }

    public static <E> DataResult<List<E>> get(List<E> data, int offset, int limit) {
        return get(PagingUtil.cutPage(data, offset, limit), data, offset, limit);
    }
}
