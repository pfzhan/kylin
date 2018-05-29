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

package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

public class ParquetPageIndexSpliceMeta implements Serializable {

    private Map<Long, List<CuboidSpliceInfo>> cuboidOffsets;

    public ParquetPageIndexSpliceMeta() {
        cuboidOffsets = new HashMap<>();
    }

    public void put(long cuboidId, long offset, int pageRangeStart, int pageRangeEnd) {
        if (cuboidOffsets.containsKey(cuboidId)) {
            cuboidOffsets.get(cuboidId).add(new CuboidSpliceInfo(offset, pageRangeStart, pageRangeEnd));
        } else {
            cuboidOffsets.put(cuboidId, Lists.newArrayList(new CuboidSpliceInfo(offset, pageRangeStart, pageRangeEnd)));
        }
    }

    public List<CuboidSpliceInfo> get(long cuboidId) {
        return cuboidOffsets.get(cuboidId);
    }

    public class CuboidSpliceInfo implements Serializable {
        public CuboidSpliceInfo(long divOffset, int divPageRangeStart, int divPageRangeEnd) {
            this.divOffset = divOffset;
            this.divPageRangeStart = divPageRangeStart;
            this.divPageRangeEnd = divPageRangeEnd;
        }

        private long divOffset;

        private int divPageRangeStart;

        private int divPageRangeEnd;

        public long getDivOffset() {
            return divOffset;
        }

        public int getDivPageRangeStart() {
            return divPageRangeStart;
        }

        public int getDivPageRangeEnd() {
            return divPageRangeEnd;
        }
    }
}
