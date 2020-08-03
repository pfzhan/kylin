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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.metadata.model.Segments;
import org.apache.spark.sql.Column;
import org.spark_project.guava.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;

public class NSparkCubingUtil {

    public static final String SEPARATOR = "_0_DOT_0_";

    public static final String SEPARATOR_TMP = "_0_DOT_TMP_0_";

    private NSparkCubingUtil() {
    }

    static String ids2Str(Set<? extends Number> ids) {
        return String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
    }

    static Set<Long> str2Longs(String str) {
        Set<Long> r = new LinkedHashSet<>();
        for (String id : str.split(",")) {
            r.add(Long.parseLong(id));
        }
        return r;
    }

    static Set<String> toSegmentIds(Set<NDataSegment> segments) {
        Set<String> r = new LinkedHashSet<>();
        for (NDataSegment seg : segments) {
            r.add(seg.getId());
        }
        return r;
    }

    public static Set<String> toIgnoredTableSet(String tableListStr) {
        if(StringUtils.isBlank(tableListStr)){
            return null;
        }
        Set<String> s = Sets.newLinkedHashSet();
        s.addAll(Arrays.asList(StringSplitter.split(tableListStr,",")));
        return s;

    }

    static Set<String> toSegmentIds(Segments<NDataSegment> segments) {
        Set<String> s = Sets.newLinkedHashSet();
        s.addAll(segments.stream().map(NDataSegment::getId).collect(Collectors.toList()));
        return s;
    }

    static Set<String> toSegmentIds(String segmentsStr) {
        Set<String> s = Sets.newLinkedHashSet();
        s.addAll(Arrays.asList(segmentsStr.split(",")));
        return s;
    }

    static Set<Long> toLayoutIds(Set<LayoutEntity> layouts) {
        Set<Long> r = new LinkedHashSet<>();
        for (LayoutEntity layout : layouts) {
            r.add(layout.getId());
        }
        return r;
    }

    static Set<Long> toLayoutIds(String layoutIdStr) {
        Set<Long> s = Sets.newLinkedHashSet();
        s.addAll(Arrays.stream(layoutIdStr.split(",")).map(Long::parseLong).collect(Collectors.toList()));
        return s;
    }

    static Set<LayoutEntity> toLayouts(IndexPlan indexPlan, Set<Long> ids) {
        Set<LayoutEntity> r = new LinkedHashSet<>();
        for (Long id : ids) {
            r.add(indexPlan.getCuboidLayout(id));
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

    public static String getStoragePath(NDataSegment nDataSegment, Long layoutId) {
        String hdfsWorkingDir = KapConfig.wrap(nDataSegment.getConfig()).getMetadataWorkingDirectory();
        return hdfsWorkingDir + getStoragePathWithoutPrefix(nDataSegment.getProject(),
                nDataSegment.getDataflow().getId(), nDataSegment.getId(), layoutId);
    }

    public static String getStoragePathWithoutPrefix(String project, String dataflowId, String segmentId,
            long layoutId) {
        return project + "/parquet/" + dataflowId + "/" + segmentId + "/" + layoutId;
    }

    private static final Pattern DOT_PATTERN = Pattern.compile("\\b([\\w`]+)\\.([\\w`]+)\\b");

    private static final Pattern LETTER_PATTERN = Pattern.compile(".*[a-zA-Z]+.*");

    private static final Pattern FLOATING_POINT = Pattern.compile("\\b[0-9]+.[0-9]*E[0-9]+\\b");

    private static final char LITERAL_QUOTE = '\'';

    public static String convertFromDot(String withDot) {
        int literalBegin = withDot.indexOf(LITERAL_QUOTE);
        if (literalBegin != -1) {
            int literalEnd = withDot.indexOf(LITERAL_QUOTE, literalBegin + 1);
            if (literalEnd != -1) {
                return doConvertFromDot(withDot.substring(0, literalBegin))
                        + withDot.substring(literalBegin, literalEnd + 1)
                        + convertFromDot(withDot.substring(literalEnd + 1));
            }
        }
        return doConvertFromDot(withDot);
    }

    public static String doConvertFromDot(String withDot) {
        Matcher m = DOT_PATTERN.matcher(withDot);
        String withoutDot = withDot;
        while (m.find()) {
            String matched = m.group();
            if (LETTER_PATTERN.matcher(matched).find() && !isFloatingPointNumber(matched)) {
                withoutDot = m.replaceFirst("$1" + SEPARATOR + "$2");
                m = DOT_PATTERN.matcher(withoutDot);
            } else {
                withoutDot = m.replaceFirst("$1" + SEPARATOR_TMP + "$2");
                m = DOT_PATTERN.matcher(withoutDot);
            }
        }
        withoutDot = withoutDot.replace(SEPARATOR_TMP, ".");
        return withoutDot.replace("`", "");
    }

    public static boolean isFloatingPointNumber(String exp) {
        Matcher matcher = FLOATING_POINT.matcher(exp);
        if (!matcher.find()) {
            return false;
        }
        try {
            Double.parseDouble(matcher.group());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static String convertToDot(String withoutDot) {
        return withoutDot.replace(SEPARATOR, ".");
    }
}
