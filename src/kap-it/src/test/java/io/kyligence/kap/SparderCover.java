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

package io.kyligence.kap;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.storage.ParquetStorage;
import io.kyligence.kap.storage.ParquetDataStorage;

/**
 *  This class use for improve java coverage with scala used, to be remove after scala coverage ready.
 */
public class SparderCover extends NLocalFileMetadataTestCase {
    private static final Logger log = LoggerFactory.getLogger(SparderCover.class);

    @Before
    public void init() {
        createTestMetadata();

    }

    @After
    public void clean() {
        cleanupTestMetadata();
    }

    @Test
    public void testParquetDataStorageCubingStorage() {
        ParquetDataStorage parquetDataStorage = new ParquetDataStorage();
        NSparkCubingEngine.NSparkCubingStorage nSparkCubingStorage = parquetDataStorage
                .adaptToBuildEngine(NSparkCubingEngine.NSparkCubingStorage.class);
        Assert.assertTrue(nSparkCubingStorage instanceof ParquetStorage);
    }
    //
    //    @Test
    //    public void testParquetDataStorage() {
    //        ParquetDataStorage parquetDataStorage = new ParquetDataStorage();
    //        NDataflow nDataflow = new NDataflow();
    //        IStorageQuery query = parquetDataStorage.createQuery(nDataflow);
    //        Assert.assertTrue(query instanceof NDataStorageQuery);
    //    }

    @Test
    public void testKapConf() {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        assert kapConfig.getListenerBusBusyThreshold() == 5000;
        assert kapConfig.getBlockNumBusyThreshold() == 5000;
    }

    @Test
    public void testHadoopUtil() throws IOException {
        FileSystem readFileSystem = HadoopUtil.getReadFileSystem();
        String scheme = readFileSystem.getScheme();
        assert scheme.equals("file");
        readFileSystem = HadoopUtil.getReadFileSystem(new Configuration());
        scheme = readFileSystem.getScheme();
        assert scheme.equals("file");
        readFileSystem = HadoopUtil.getWorkingFileSystem(new Configuration());
        scheme = readFileSystem.getScheme();
        assert scheme.equals("file");
        readFileSystem = HadoopUtil.getWorkingFileSystem();
        scheme = readFileSystem.getScheme();
        assert scheme.equals("file");
    }
}
