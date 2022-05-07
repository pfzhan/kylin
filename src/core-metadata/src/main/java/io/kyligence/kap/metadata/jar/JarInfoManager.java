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

package io.kyligence.kap.metadata.jar;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JarInfoManager {

    private KylinConfig kylinConfig;
    private CachedCrudAssist<JarInfo> crud;

    public static JarInfoManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, JarInfoManager.class);
    }

    static JarInfoManager newInstance(KylinConfig kylinConfig, String project) {
        return new JarInfoManager(kylinConfig, project);
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    private JarInfoManager(KylinConfig kylinConfig, String project) {
        this.kylinConfig = kylinConfig;
        String resourceRootPath = String.format(Locale.ROOT, "/%s%s", project, ResourceStore.JAR_RESOURCE_ROOT);
        this.crud = new CachedCrudAssist<JarInfo>(getStore(), resourceRootPath, JarInfo.class) {
            @Override
            protected JarInfo initEntityAfterReload(JarInfo entity, String resourceName) {
                return entity;
            }
        };
        crud.reloadAll();
    }

    public JarInfo getJarInfo(String jarTypeName) {
        if (org.apache.commons.lang.StringUtils.isEmpty(jarTypeName)) {
            return null;
        }
        return crud.get(jarTypeName);
    }

    public JarInfo createJarInfo(JarInfo jarInfo) {
        if (Objects.isNull(jarInfo) || StringUtils.isEmpty(jarInfo.resourceName())) {
            throw new IllegalArgumentException("jar info is null or resourceName is null");
        }
        if (crud.contains(jarInfo.resourceName())) {
            throw new IllegalArgumentException("jarInfo '" + jarInfo.resourceName() + "' already exists");
        }

        jarInfo.updateRandomUuid();
        return crud.save(jarInfo);
    }

    public JarInfo updateJarInfo(JarInfo jarInfo) {
        if (!crud.contains(jarInfo.resourceName())) {
            throw new IllegalArgumentException("JarInfo '" + jarInfo.resourceName() + "' does not exist.");
        }
        return crud.save(jarInfo);
    }

    public JarInfo removeJarInfo(String jarTypeName) {
        JarInfo jarInfo = getJarInfo(jarTypeName);
        if (Objects.isNull(jarInfo)) {
            log.warn("Removing JarInfo '{}' does not exist", jarTypeName);
            return null;
        }
        crud.delete(jarInfo);
        log.info("Removing JarInfo '{}' success", jarInfo);
        return jarInfo;
    }

    public List<JarInfo> listJarInfo() {
        return new ArrayList<>(crud.listAll());
    }

    public List<JarInfo> listJarInfoByType(JarTypeEnum jarTypeEnum) {
        return listJarInfo().stream().filter(jarInfo -> jarTypeEnum == jarInfo.getJarType())
                .collect(Collectors.toList());
    }

}
