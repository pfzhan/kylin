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
package io.kyligence.kap.secondstorage.util;

import org.apache.kylin.metadata.model.SegmentRange;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SecondStorageDateUtils {

    public static boolean isInfinite(long start, long end) {
        return start == 0 && end == Long.MAX_VALUE;
    }

    /**
     * @param start Include
     * @param end   exclude
     * @return
     */
    public static List<String> splitByDayStr(long start, long end) {
        return splitByDay(start, end).stream().map(Objects::toString).collect(Collectors.toList());
    }

    public static List<Date> splitByDay(SegmentRange<Long> range) {
        return splitByDay(range.getStart(), range.getEnd());
    }

    public static List<Date> splitByDay(long start, long end) {
        if (end == Long.MAX_VALUE) {
            throw new IllegalArgumentException("segmentRange end is invalid.");
        }
        List<Date> partitions = new ArrayList<>();
        while (start < end) {
            partitions.add(new Date(start));
            start = start + 24 * 60 * 60 * 1000;
        }
        return partitions;
    }
}
