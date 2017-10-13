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

package io.kyligence.kap.storage.parquet.format;

import com.google.common.primitives.Longs;

public interface ParquetFormatConstants {
    String KYLIN_OUTPUT_DIR = "io.kylin.job.output.path";

    String KYLIN_SCAN_PROPERTIES = "io.kylin.storage.parquet.scan.properties";
    String KYLIN_SCAN_REQUEST_BYTES = "io.kylin.storage.parquet.scan.request";
    String KYLIN_SCAN_REQUIRED_PARQUET_COLUMNS = "io.kylin.storage.parquet.scan.parquetcolumns";
    String KYLIN_GT_MAX_LENGTH = "io.kylin.storage.parquet.scan.gtrecord.maxlength";
    String KYLIN_USE_INVERTED_INDEX = "io.kylin.storage.parquet.scan.useii";
    String KYLIN_TARBALL_READ_STRATEGY = "io.kylin.storage.parquet.read.strategy";
    String KYLIN_REQUIRED_CUBOIDS = "io.kylin.storage.parquet.read.required-cuboids";
    String KYLIN_BINARY_FILTER = "io.kylin.storage.parquet.read.binary-filter";

    int KYLIN_PARQUET_TARBALL_HEADER_SIZE = Longs.BYTES;
    int KYLIN_DEFAULT_GT_MAX_LENGTH = 1024 * 1024; // 1M
}
