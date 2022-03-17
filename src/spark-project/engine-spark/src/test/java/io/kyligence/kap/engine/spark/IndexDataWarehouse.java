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
package io.kyligence.kap.engine.spark;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ZipFileUtils;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.util.TestUtils;
import lombok.AllArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class IndexDataWarehouse {

    private KylinConfig kylinConfig;

    private String project;

    private String suffix;

    public void reuseBuildData(File outputFolder) throws IOException {
        FileUtils.deleteQuietly(outputFolder);
        ZipFileUtils.decompressZipFile(outputFolder.getAbsolutePath() + ".zip",
                outputFolder.getParentFile().getAbsolutePath());
        FileUtils.copyDirectory(new File(outputFolder, "hdfs"),
                new File(kylinConfig.getHdfsWorkingDirectory().substring(7)));

        val buildConfig = KylinConfig.createKylinConfig(kylinConfig);
        buildConfig.setMetadataUrl(outputFolder.getAbsolutePath() + "/metadata");
        val buildStore = ResourceStore.getKylinMetaStore(buildConfig);
        val store = ResourceStore.getKylinMetaStore(kylinConfig);
        for (String key : store.listResourcesRecursively("/" + project)) {
            store.deleteResource(key);
        }
        for (String key : buildStore.listResourcesRecursively("/" + project)) {
            val raw = buildStore.getResource(key);
            store.deleteResource(key);
            store.putResourceWithoutCheck(key, raw.getByteSource(), System.currentTimeMillis(), 100);
        }
        FileUtils.deleteQuietly(outputFolder);
        log.info("reuse data succeed for {}", outputFolder);
    }

    boolean reuseBuildData() {
        if (!TestUtils.isSkipBuild()) {
            return false;
        }
        try {
            val method = findTestMethod();
            val inputFolder = new File(kylinConfig.getMetadataUrlPrefix()).getParentFile();
            val outputFolder = getOutputFolder(inputFolder, method);
            reuseBuildData(outputFolder);
        } catch (IOException | NoSuchElementException e) {
            log.warn("reuse data failed", e);
            return false;
        }
        return true;
    }

    void persistBuildData() {
        if (!TestUtils.isPersistBuild()) {
            return;
        }
        try {
            val method = findTestMethod();
            val inputFolder = new File(kylinConfig.getMetadataUrlPrefix()).getParentFile();
            val outputFolder = getOutputFolder(inputFolder, method);

            FileUtils.deleteQuietly(outputFolder);
            FileUtils.deleteQuietly(new File(outputFolder.getAbsolutePath() + ".zip"));

            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            val outputConfig = KylinConfig.createKylinConfig(kylinConfig);
            outputConfig.setMetadataUrl(outputFolder.getCanonicalPath() + "/metadata");
            MetadataStore.createMetadataStore(outputConfig).dump(resourceStore);
            //            FileUtils.copyDirectory(new File(inputFolder, "data"), new File(outputFolder, "data"));
            FileUtils.copyDirectory(new File(kylinConfig.getHdfsWorkingDirectory().substring(7)),
                    new File(outputFolder, "hdfs"));
            ZipFileUtils.compressZipFile(outputFolder.getAbsolutePath(), outputFolder.getAbsolutePath() + ".zip");
            log.info("build data succeed for {}", outputFolder.getName());
            FileUtils.deleteQuietly(outputFolder);
        } catch (Exception e) {
            log.warn("build data failed", e);
        }
    }

    Method findTestMethod() {
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .filter(ele -> ele.getClassName().startsWith("io.kyligence.kap")) //
                .map(ele -> {
                    try {
                        return ReflectionUtils.findMethod(Class.forName(ele.getClassName()), ele.getMethodName());
                    } catch (ClassNotFoundException e) {
                        return null;
                    }
                }) //
                .filter(m -> m != null && (m.isAnnotationPresent(Test.class)
                        || m.isAnnotationPresent(org.junit.jupiter.api.Test.class)))
                .reduce((first, second) -> second) //
                .get();
    }

    File getOutputFolder(File inputFolder, Method method) {
        return new File(inputFolder.getParentFile(),
                method.getDeclaringClass().getCanonicalName() + "." + method.getName() + "." + suffix);
    }
}
