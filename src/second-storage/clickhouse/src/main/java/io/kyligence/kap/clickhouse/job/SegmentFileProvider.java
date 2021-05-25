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

package io.kyligence.kap.clickhouse.job;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SegmentFileProvider implements FileProvider {
    private final String rootPath;

    public SegmentFileProvider(String rootPath) {
        this.rootPath = rootPath;
    }

    @Override
    public List<FileStatus> getAllFilePaths() {
        List<FileStatus> paths = new ArrayList<>();
        final FileSystem fs = HadoopUtil.getWorkingFileSystem();
        try {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(rootPath), false);
            while (it.hasNext()) {
                LocatedFileStatus fileStatus = it.next();
                if (!fileStatus.getPath().getName().endsWith("parquet")) {
                    continue;
                }
                paths.add(FileStatus.builder()
                        .path(fileStatus.getPath().toString())
                        .len(fileStatus.getLen())
                        .build());
            }
        } catch (IOException e) {
            return ExceptionUtils.rethrow(new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, e));
        }
        return paths;
    }

}
