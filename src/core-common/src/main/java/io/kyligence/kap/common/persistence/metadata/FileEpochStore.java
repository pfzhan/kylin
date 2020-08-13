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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileEpochStore extends EpochStore {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final File root;

    public static EpochStore getEpochStore(KylinConfig config) {
        return Singletons.getInstance(FileEpochStore.class, clz -> new FileEpochStore(config));
    }

    private FileEpochStore(KylinConfig kylinConfig) {
        root = Paths
                .get(Paths.get(kylinConfig.getMetadataUrlPrefix()).getParent().toFile().getAbsolutePath(), EPOCH_SUFFIX)
                .toFile().getAbsoluteFile();
    }

    @Override
    public void saveOrUpdate(Epoch epoch) {
        if (!root.exists()) {
            root.mkdirs();
        }
        try {
            epoch.setMvcc(epoch.getMvcc() + 1);
            objectMapper.writeValue(new File(root, epoch.getEpochTarget()), epoch);
        } catch (IOException e) {
            log.warn("Save or update epoch {} failed", epoch, e);
        }
    }

    @Override
    public Epoch getEpoch(String epochTarget) {
        File file = new File(root, epochTarget);
        if (file.exists()) {
            try {
                return objectMapper.readValue(file, Epoch.class);
            } catch (IOException e) {
                log.warn("Get epoch {} failed", epochTarget, e);
            }
        }
        return null;
    }

    @Override
    public List<Epoch> list() {
        List<Epoch> results = new ArrayList<>();

        File[] files = root.listFiles();
        if (files != null) {
            for (File file : files) {
                try {
                    results.add(objectMapper.readValue(file, Epoch.class));
                } catch (IOException e) {
                    log.warn("Get epoch from file {} failed", file.getAbsolutePath(), e);
                }
            }
        }
        return results;
    }

    @Override
    public void delete(String epochTarget) {
        File file = new File(root, epochTarget);
        if (file.exists()) {
            try {
                Files.delete(file.toPath());
            } catch (IOException e) {
                log.warn("Delete epoch {} failed", epochTarget);
            }
        }
    }

    @Override
    public void createIfNotExist() throws Exception {
        if (!root.exists()) {
            root.mkdirs();
        }
    }
}