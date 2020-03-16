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
package org.apache.kylin.common.storage;

import lombok.Data;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;

// for reflection
public class DefaultStorageProvider implements IStorageProvider {
    /**
     * Warning: different cloud provider may not return full ContentSummary,
     * only return file length and count now.
     * @param fileSystem
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public ContentSummary getContentSummary(FileSystem fileSystem, Path path) throws IOException {
        try {
            return fileSystem.getContentSummary(path);
        } catch (AccessControlException ae) {
            ContentSummaryBean bean = recursive(fileSystem, path);
            ContentSummary.Builder builder = new ContentSummary.Builder();
            return builder.fileCount(bean.getFileCount()).length(bean.getLength()).build();
        }
    }

    @Data
    class ContentSummaryBean {
        long fileCount = 0;
        long length = 0;
    }

    public ContentSummaryBean recursive(FileSystem fs, Path path) throws IOException {
        ContentSummaryBean result = new ContentSummaryBean();
        for (FileStatus fileStatus : fs.listStatus(path)) {
            if (fileStatus.isDirectory()) {
                ContentSummaryBean bean = recursive(fs, fileStatus.getPath());
                result.setFileCount(result.getFileCount() + bean.getFileCount());
                result.setLength(result.getLength() + bean.getLength());
            } else {
                result.setFileCount(result.getFileCount() + 1);
                result.setLength(result.getLength() + fileStatus.getLen());
            }
        }

        return result;
    }
}
