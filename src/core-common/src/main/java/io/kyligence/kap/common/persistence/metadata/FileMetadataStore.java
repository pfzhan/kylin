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
package io.kyligence.kap.common.persistence.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;

import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileMetadataStore extends MetadataStore {

    private final File root;

    public FileMetadataStore(KylinConfig kylinConfig) {
        super(kylinConfig);
        root = new File(kylinConfig.getMetadataUrl().getIdentifier()).getAbsoluteFile();
    }

    @Override
    protected void save(String path, ByteSource bs, long ts, long mvcc) throws Exception {
        File f = file(path);
        f.getParentFile().mkdirs();
        if (bs == null) {
            FileUtils.deleteQuietly(f);
            return;
        }
        try (FileOutputStream out = new FileOutputStream(f)) {
            IOUtils.copy(bs.openStream(), out);
        }

        if (!f.setLastModified(ts)) {
            log.info("{} modified time change failed", f);
        }
    }

    @Override
    public NavigableSet<String> list(String subPath) {
        TreeSet<String> result = Sets.newTreeSet();
        val scanFolder = new File(root, subPath);
        if (!scanFolder.exists()) {
            return result;
        }
        val files = FileUtils.listFiles(scanFolder, null, true);
        for (File file : files) {
            result.add(file.getPath().replace(scanFolder.getPath(), ""));
        }
        return result;
    }

    @Override
    public RawResource load(String path) throws IOException {
        val f = new File(root, path);
        val resPath = f.getPath().replace(root.getPath() + path, "");
        try (FileInputStream in = new FileInputStream(f)) {
            val bs = ByteStreams.asByteSource(IOUtils.toByteArray(in));
            return new RawResource(resPath, bs, f.lastModified(), 0);
        }
    }

    private File file(String resPath) {
        if (resPath.equals("/"))
            return root;
        else
            return new File(root, resPath);
    }

}