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
package io.kyligence.kap.rest.service;

import java.io.File;
import java.net.URI;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.common.util.TempMetadataBuilder;
import lombok.val;

public class ImageArchiveServiceTest extends NLocalFileMetadataTestCase {

    private ImageArchiveService imageArchiveService = new ImageArchiveService();

    @Before
    public void init() throws Exception {
        staticCreateTestMetadata();
        getTestConfig().setProperty("kylin.metadata.url", "test_meta@hdfs,mq=mock");
        val currentImagePath = new Path(HadoopUtil.getLatestImagePath(getTestConfig()));
        val rootPath = currentImagePath.getParent();
        val fs = HadoopUtil.getFileSystem(rootPath);
        fs.copyFromLocalFile(new Path(TempMetadataBuilder.N_KAP_META_TEST_DATA + "/metadata"),
                new Path(currentImagePath, "metadata"));
        for (int i = 0; i < 4; i++) {
            val archivePath = new Path(rootPath, "archive-2018-10-1" + i);
            fs.copyFromLocalFile(new Path(TempMetadataBuilder.N_KAP_META_TEST_DATA + "/metadata"),
                    new Path(archivePath, "metadata"));
            fs.setTimes(archivePath, System.currentTimeMillis() - 3600 * 1000 + i * 10 * 1000,
                    System.currentTimeMillis() - 3600 * 1000 + i * 10 * 1000);
        }
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testArchive() throws Exception {
        imageArchiveService.archive();
        val currentPath = new Path(HadoopUtil.getLatestImagePath(getTestConfig()));
        val rootPath = currentPath.getParent();
        val fs = HadoopUtil.getFileSystem(rootPath);
        val fileStatus = fs.listStatus(rootPath);
        Assert.assertEquals(5, fileStatus.length);
        val paths = Stream.of(fileStatus).sorted(Comparator.comparing(FileStatus::getModificationTime))
                .map(f -> f.getPath().getName()).collect(Collectors.toList());
        Assert.assertEquals("latest", paths.get(paths.size() - 1));
        Assert.assertEquals("archive-2018-10-11", paths.get(0));
        val map = JsonUtil.readValue(new File(new URI(currentPath.toString() + "/events.json")), Map.class);
    }
}
