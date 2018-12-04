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

package io.kyligence.kap.engine.spark.job;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KapConfig;
import org.apache.spark.sql.Column;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegDetails;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;

public class NSparkCubingUtil {
    public static final String SEPARATOR = "_0_DOT_0_";

    public static String ids2Str(Set<? extends Number> ids) {
        StringBuilder sb = new StringBuilder();
        for (Number i : ids) {
            if (sb.length() > 0)
                sb.append(",");
            sb.append(i);
        }
        return sb.toString();
    }

    public static Set<Long> str2Longs(String str) {
        Set<Long> r = new LinkedHashSet<>();
        for (String id : str.split(",")) {
            r.add(Long.parseLong(id));
        }
        return r;
    }

    public static Set<Integer> str2Ints(String str) {
        Set<Integer> r = new LinkedHashSet<>();
        for (String id : str.split(",")) {
            r.add(Integer.parseInt(id));
        }
        return r;
    }

    public static Set<Integer> toSegmentIds(Set<NDataSegment> segments) {
        Set<Integer> r = new LinkedHashSet<>();
        for (NDataSegment seg : segments) {
            r.add(seg.getId());
        }
        return r;
    }

    public static Set<Long> toCuboidLayoutIds(Set<NCuboidLayout> cuboids) {
        Set<Long> r = new LinkedHashSet<>();
        for (NCuboidLayout cl : cuboids) {
            r.add(cl.getId());
        }
        return r;
    }

    public static Set<NDataSegment> toSegments(NDataflow dataflow, Set<Integer> ids) {
        Set<NDataSegment> r = new LinkedHashSet<>();
        for (int id : ids) {
            r.add(dataflow.getSegment(id));
        }
        return r;
    }

    public static Set<NCuboidLayout> toLayouts(NCubePlan cubePlan, Set<Long> ids) {
        Set<NCuboidLayout> r = new LinkedHashSet<>();
        for (Long id : ids) {
            r.add(cubePlan.getCuboidLayout(id));
        }
        return r;
    }

    public static Column[] getColumns(Set<Integer> indices1, Set<Integer> indices2) {
        Set<Integer> ret = new LinkedHashSet<>();
        ret.addAll(indices1);
        ret.addAll(indices2);
        return getColumns(ret);
    }

    public static Column[] getColumns(Set<Integer> indices) {
        Column[] ret = new Column[indices.size()];
        int index = 0;
        for (Integer i : indices) {
            ret[index] = new Column(String.valueOf(i));
            index++;
        }
        return ret;
    }

    public static Column[] getColumns(List<Integer> indices) {
        Column[] ret = new Column[indices.size()];
        int index = 0;
        for (Integer i : indices) {
            ret[index] = new Column(String.valueOf(i));
            index++;
        }
        return ret;
    }

    public static String formatSQL(String sql) {
        return String.format("(%s) t", sql);
    }

    public static String getStoragePath(NDataCuboid dataCuboid) {
        NDataSegDetails segDetails = dataCuboid.getSegDetails();
        KapConfig config = KapConfig.wrap(dataCuboid.getConfig());
        String hdfsWorkingDir = config.getReadHdfsWorkingDirectory();
        String path = hdfsWorkingDir + segDetails.getProject() + "/parquet/" + segDetails.getDataSegment().getDataflow().getUuid() + "/"
                + segDetails.getSegmentId() + "/" + dataCuboid.getCuboidLayoutId();
        return path;
    }

    public static String convertFromDot(String withDot) {
        return withDot.replace(".", SEPARATOR);
    }

    public static String convertToDot(String withoutDot) {
        return withoutDot.replace(SEPARATOR, ".");
    }
}
