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

package io.kyligence.kap.engine.mr;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.NavigableSet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;

public class HDFSResourceStore extends ResourceStore {
    public HDFSResourceStore(KylinConfig kylinConfig) {
        super(kylinConfig);
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException {
        return null;
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        return false;
    }

    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive) throws IOException {
        return null;
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        InputStream inputStream = fs.open(hdfsPath(resPath, kylinConfig));
        RawResource result = new RawResource(inputStream, 0);
        return result;
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        return 0;
    }

    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        FSDataOutputStream os = fs.create(hdfsPath(resPath, kylinConfig), false);
        BufferedOutputStream bos = new BufferedOutputStream(os);
        IOUtils.copy(content, bos);
        bos.close();
    }

    public static Path hdfsPath(String resourcePath, KylinConfig conf) {
        return new Path(conf.getHdfsWorkingDirectory(), resourcePath.substring(1));
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException {
        return 0;
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {

    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return null;
    }
}
