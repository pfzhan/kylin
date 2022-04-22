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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.metadata.model.Segments;
import org.apache.spark.sql.Column;
import org.sparkproject.guava.collect.Sets;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.val;

public class NSparkCubingUtil {

    public static final String SEPARATOR = "_0_DOT_0_";

    public static final String CC_SEPARATOR = "_0_DOT_CC_0_";

    public static final String SEPARATOR_TMP = "_0_DOT_TMP_0_";

    public static final String BACKTICK_TMP = "_0_BACKTICK_0_";

    private NSparkCubingUtil() {
    }

    public static String ids2Str(Set<? extends Number> ids) {
        return String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
    }

    public static Set<Long> str2Longs(String str) {
        Set<Long> r = new LinkedHashSet<>();
        for (String id : str.split(",")) {
            r.add(Long.parseLong(id));
        }
        return r;
    }

    public static Set<String> toSegmentIds(Set<NDataSegment> segments) {
        Set<String> r = new LinkedHashSet<>();
        for (NDataSegment seg : segments) {
            r.add(seg.getId());
        }
        return r;
    }

    public static Set<String> toIgnoredTableSet(String tableListStr) {
        if (StringUtils.isBlank(tableListStr)) {
            return Sets.newLinkedHashSet();
        }
        Set<String> s = Sets.newLinkedHashSet();
        s.addAll(Arrays.asList(StringSplitter.split(tableListStr, ",")));
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

    public static Set<LayoutEntity> toLayouts(IndexPlan indexPlan, Set<Long> layouts) {
        return layouts.stream().map(indexPlan::getLayoutEntity).filter(Objects::nonNull)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @SafeVarargs
    public static Set<Integer> combineIndices(Set<Integer>... items) {
        Set<Integer> combined = new LinkedHashSet<>();
        for (Set<Integer> single : items) {
            combined.addAll(single);
        }
        return combined;
    }

    @SafeVarargs
    public static Column[] getColumns(Set<Integer>... items) {
        Set<Integer> indices = combineIndices(items);
        Column[] ret = new Column[indices.size()];
        int index = 0;
        for (Integer i : indices) {
            ret[index] = new Column(String.valueOf(i));
            index++;
        }
        return ret;
    }

    public static String getStoragePath(NDataSegment nDataSegment, Long layoutId, Long bucketId) {
        String hdfsWorkingDir = KapConfig.wrap(nDataSegment.getConfig()).getMetadataWorkingDirectory();
        return hdfsWorkingDir + getStoragePathWithoutPrefix(nDataSegment.getProject(),
                nDataSegment.getDataflow().getId(), nDataSegment.getId(), layoutId, bucketId);
    }

    public static String getStoragePath(NDataSegment nDataSegment, Long layoutId) {
        return getStoragePath(nDataSegment, layoutId, null);
    }

    public static String getStoragePath(NDataSegment nDataSegment) {
        return getStoragePath(nDataSegment, null, null);
    }

    public static String getStoragePathWithoutPrefix(String project, String dataflowId, String segmentId,
            Long layoutId) {
        return getStoragePathWithoutPrefix(project, dataflowId, segmentId, layoutId, null);
    }

    public static String getStoragePathWithoutPrefix(String project, String dataflowId, String segmentId, Long layoutId,
            Long bucketId) {
        final String parquet = "/parquet/";
        if (layoutId == null) {
            return project + parquet + dataflowId + "/" + segmentId;
        }
        if (bucketId == null) {
            return project + parquet + dataflowId + "/" + segmentId + "/" + layoutId;
        } else {
            return project + parquet + dataflowId + "/" + segmentId + "/" + layoutId + "/" + bucketId;
        }
    }

    private static final Pattern DOT_PATTERN = Pattern.compile("\\b([\\w`]+)\\.([\\w`]+)\\b");

    private static final Pattern UDF_FUNCTION_PATTERN = Pattern.compile("\\b([\\w`]+)\\.([\\w`]+)\\b([\\(]+)");
    private static final String COLUMN_NAME_PATTERN = "[^:.`]+";
    private static final String COLUMN_NAME_PATTERN_ONLY_WORD = "[\\w]+";
    private static final String TABLE_NAME_PATTERN = "[a-zA-Z0-9_]+";

    private static final char LITERAL_QUOTE = '\'';

    public static String convertFromDot(String withDot) {
        int literalBegin = withDot.indexOf(LITERAL_QUOTE);
        if (literalBegin != -1) {
            int literalEnd = withDot.indexOf(LITERAL_QUOTE, literalBegin + 1);
            if (literalEnd != -1) {
                return doConvertFromDot(withDot.substring(0, literalBegin), false)
                        + withDot.substring(literalBegin, literalEnd + 1)
                        + convertFromDot(withDot.substring(literalEnd + 1));
            }
        }
        return doConvertFromDot(withDot, false);
    }

    private static String doConvertComputedColumnFromDot(String exp) {
        String withoutDot = exp;
        Matcher m = UDF_FUNCTION_PATTERN.matcher(exp);
        while (m.find()) {
            withoutDot = m.replaceFirst("$1" + CC_SEPARATOR + "$2(");
            m = UDF_FUNCTION_PATTERN.matcher(withoutDot);
        }
        return withoutDot;
    }

    public static String convertFromDotWithBackticks(String withDot) {
        int literalBegin = withDot.indexOf(LITERAL_QUOTE);
        if (literalBegin != -1) {
            int literalEnd = withDot.indexOf(LITERAL_QUOTE, literalBegin + 1);
            if (literalEnd != -1) {
                return doConvertFromDot(withDot.substring(0, literalBegin), true)
                        + withDot.substring(literalBegin, literalEnd + 1)
                        + convertFromDot(withDot.substring(literalEnd + 1));
            }
        }
        return doConvertFromDot(withDot, true);
    }

    public static String doConvertFromDot(String input, boolean addDoubleQuota) {
        String  convertCCResult= doConvertComputedColumnFromDot(input);
        //        find floating point, replace . with tmp_dot
        String dot_to_tmp_dot = convertFloatingPoint(convertCCResult);
        //        find pattern `_`.`_`,replace . with dot
        String replaceDotBetweenBackTick = dot_to_tmp_dot;
        replaceDotBetweenBackTick = replacePattern(replaceDotBetweenBackTick,getLetterPatternWithBacktick(),addDoubleQuota);
        //        remove `
        String removeBackTickResult = replaceDotBetweenBackTick.replace("`", "");
        //        find pattern table.column, replace . with _0_DOT_0_
        String replaceDotBetweenBackTableColumn = replacePattern(removeBackTickResult, getLetterPatternWithoutBacktick(),addDoubleQuota);
        //         revert tmp_dot to .
        String revertTmpDot = replaceDotBetweenBackTableColumn.replace(SEPARATOR_TMP, ".").replace(BACKTICK_TMP,"`").replace(CC_SEPARATOR, ".");
        return revertTmpDot;
    }

    private static String replacePattern(String input,Pattern pattern,boolean addBackTick) {
        String replace = "$1" + SEPARATOR + "$2";
        if (addBackTick) {
            replace = BACKTICK_TMP + replace + BACKTICK_TMP;
        }
        Matcher matcher = pattern.matcher(input);
        while (matcher.find()) {
            input = matcher.replaceFirst(replace);
            matcher = pattern.matcher(input);
        }
        return input;
    }

    public static boolean isFloatingPointNumber(String exp) {
        try {
            Double.parseDouble(exp);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static String convertToDot(String withoutDot) {
        return withoutDot.replace(SEPARATOR, ".");
    }

    public static Map<Long, LayoutEntity> toLayoutMap(IndexPlan indexPlan, Set<Long> layoutIds) {
        val layouts = toLayouts(indexPlan, layoutIds).stream().filter(Objects::nonNull).collect(Collectors.toSet());
        Map<Long, LayoutEntity> map = Maps.newHashMap();
        layouts.forEach(layout -> map.put(layout.getId(), layout));
        return map;
    }

    public static String convertFloatingPoint(String withDot) {
        Matcher m = DOT_PATTERN.matcher(withDot);
        List<Integer> positions = new ArrayList<>();

        while (m.find()) {
            String matched = m.group();
            if (isFloatingPointNumber(matched)) {
                positions.add(m.start() + m.group(1).length());
            }
        }
        String res = "";
        if (positions.isEmpty()) {
            res = withDot;
        } else {
            int last = 0;
            for (int i : positions) {
                res += withDot.substring(last, i) + SEPARATOR_TMP;
                last = i + 1;
            }
            res += withDot.substring(last);
        }
        return res;
    }

    private static Pattern getLetterPatternWithBacktick(){
        if (KylinConfig.getInstanceFromEnv().isSupportSpecialSymbolInHiveColumn()) {
            return Pattern.compile("(`" + TABLE_NAME_PATTERN + "`)" + "\\." + "(`" + COLUMN_NAME_PATTERN + "`)");
        }else {
            return Pattern.compile("(`" + TABLE_NAME_PATTERN + "`)" + "\\." + "(`" + COLUMN_NAME_PATTERN_ONLY_WORD + "`)");
        }
    }
    private static Pattern getLetterPatternWithoutBacktick(){
        if (KylinConfig.getInstanceFromEnv().isSupportSpecialSymbolInHiveColumn()) {
            return Pattern.compile("(" + TABLE_NAME_PATTERN + ")" + "\\." + "(" + COLUMN_NAME_PATTERN + ")");
        } else {
            return Pattern.compile("(" + TABLE_NAME_PATTERN + ")" + "\\." + "(" + COLUMN_NAME_PATTERN_ONLY_WORD + ")");
        }
    }

}
