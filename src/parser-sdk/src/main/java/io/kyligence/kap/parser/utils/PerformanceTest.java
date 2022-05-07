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

package io.kyligence.kap.parser.utils;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;

import io.kyligence.kap.parser.AbstractDataParser;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PerformanceTest {

    private static final long DATA_SIZE_20K = 20_000L;
    private static final long DATA_SIZE_40K = 40_000L;
    private static final long DATA_SIZE_60K = 60_000L;

    private PerformanceTest() {
    }

    public static <I> Long test20K(I input, AbstractDataParser<I> parser) {
        return testParse(input, parser, DATA_SIZE_20K);
    }

    public static <I> Long test40K(I input, AbstractDataParser<I> parser) {
        return testParse(input, parser, DATA_SIZE_40K);
    }

    public static <I> Long test60K(I input, AbstractDataParser<I> parser) {
        return testParse(input, parser, DATA_SIZE_60K);
    }

    public static <I> Long testWithSize(I input, AbstractDataParser<I> parser, long size) {
        checkSize(size);
        return testParse(input, parser, size);
    }

    private static <I> Long testParse(I input, AbstractDataParser<I> parser, long size) {
        StopWatch stopWatch = new StopWatch("Parse Testing");
        try {
            stopWatch.start();
            for (int i = 0; i < size; i++) {
                parser.process(input);
            }
            stopWatch.stop();
        } catch (Exception e) {
            log.error("Abnormal data during test", e);
        }
        return stopWatch.getTime(TimeUnit.MILLISECONDS);
    }

    private static void checkSize(long size) {
        if (size <= 0) {
            throw new IllegalArgumentException("The number of test data size should be >= 0");
        }
    }
}
