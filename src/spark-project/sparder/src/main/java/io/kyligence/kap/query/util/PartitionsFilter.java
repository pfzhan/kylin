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
package io.kyligence.kap.query.util;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;

public class PartitionsFilter implements PathFilter, Configurable, Serializable {
    public static final String PARTITION_COL = "partition_col";
    public static final String PARTITIONS = "partitions";

    private Set<String> partitions = Sets.newHashSet();

    @Override
    public boolean accept(Path path) {
        if (partitions.contains(path.getParent().getName())) {
            return true;
        }
        return false;
    }

    @Override
    public void setConf(Configuration conf) {
        String colName = conf.get(PARTITION_COL);
        partitions = toPartitions(colName, conf.get(PARTITIONS));
    }

    private Set<String> toPartitions(String colName, String conf) {
        Set<String> partitionNames = new LinkedHashSet<>();
        for (String id : conf.split(",")) {
            partitionNames.add(colName + "=" + id);
        }
        return partitionNames;
    }

    @Override
    public Configuration getConf() {
        return null;
    }

}