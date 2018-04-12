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

 
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.


 */

package org.apache.kylin.source;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A table that can be read.
 */
public interface IReadableTable {

    /**
     * Returns a reader to read the table.
     */
    public TableReader getReader() throws IOException;

    /**
     * Used to detect table modifications.
     */
    public TableSignature getSignature() throws IOException;

    public boolean exists() throws IOException;

    public interface TableReader extends Closeable {

        /**
         * Move to the next row, return false if no more record.
         */
        public boolean next() throws IOException;

        /**
         * Get the current row.
         */
        public String[] getRow();

    }

    // ============================================================================

    @SuppressWarnings("serial")
    @JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
    public class TableSignature implements Serializable {

        @JsonProperty("path")
        private String path;
        @JsonProperty("size")
        private long size;
        @JsonProperty("last_modified_time")
        private long lastModifiedTime;

        // for JSON serialization
        public TableSignature() {
        }

        public TableSignature(String path, long size, long lastModifiedTime) {
            super();
            this.path = path;
            this.size = size;
            this.lastModifiedTime = lastModifiedTime;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public void setLastModifiedTime(long lastModifiedTime) {
            this.lastModifiedTime = lastModifiedTime;
        }

        public String getPath() {
            return path;
        }

        public long getSize() {
            return size;
        }

        public long getLastModifiedTime() {
            return lastModifiedTime;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (lastModifiedTime ^ (lastModifiedTime >>> 32));
            result = prime * result + ((path == null) ? 0 : path.hashCode());
            result = prime * result + (int) (size ^ (size >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TableSignature other = (TableSignature) obj;
            if (lastModifiedTime != other.lastModifiedTime)
                return false;
            if (path == null) {
                if (other.path != null)
                    return false;
            } else if (!path.equals(other.path))
                return false;
            if (size != other.size)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "FileSignature [path=" + path + ", size=" + size + ", lastModifiedTime=" + lastModifiedTime + "]";
        }

    }

}
