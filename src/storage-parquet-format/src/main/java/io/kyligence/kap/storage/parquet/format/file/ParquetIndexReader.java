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

package io.kyligence.kap.storage.parquet.format.file;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;

public class ParquetIndexReader {
    private FSDataInputStream is;
    private Map<String, Long> indexMap;
    private Map<Integer, List<GroupOffsetPair>> indexListMap;

    public ParquetIndexReader(Configuration config, Path path) throws IOException {
        FileSystem fileSystem = HadoopUtil.getFileSystem(path, config);
        is = fileSystem.open(path);
        indexListMap = new HashMap<Integer, List<GroupOffsetPair>>();
        indexMap = getIndex(indexListMap);
    }

    public long getOffset(int group, int column, int page) {
        return indexMap.get(getIndexKey(group, column, page));
    }

    public List<GroupOffsetPair> getOffsets(int column) {
        return indexListMap.get(column);
    }

    public void close() throws IOException {
        is.close();
    }

    private Map<String, Long> getIndex(Map<Integer, List<GroupOffsetPair>> indexListMap) throws IOException {
        int group, column, page;
        long offset;

        Map<String, Long> indexMap = new HashMap<String, Long>();

        try {
            while (true) {
                group = is.readInt();
                column = is.readInt();
                page = is.readInt();
                offset = is.readLong();
                indexMap.put(getIndexKey(group, column, page), offset);

                if (!indexListMap.containsKey(column)) {
                    indexListMap.put(column, new ArrayList<GroupOffsetPair>());
                }
                indexListMap.get(column).add(new GroupOffsetPair(group, offset));
            }
        } catch (EOFException ex) {
            //do nothing?
        }
        return indexMap;
    }

    private String getIndexKey(int group, int column, int page) {
        return group + "," + column + "," + page;
    }

    public static Path getIndexPath(Path path) {
        return new Path(path.toString() + "offset");
    }

    public class GroupOffsetPair {
        private int group;
        private long offset;

        public GroupOffsetPair(int group, long offset) {
            this.group = group;
            this.offset = offset;
        }

        public int getGroup() {
            return group;
        }

        public long getOffset() {
            return offset;
        }
    }
}
