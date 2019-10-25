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

package io.kyligence.kap.rest.config.initialize;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.transaction.EventListenerRegistry;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;

public class AclTCRListener implements EventListenerRegistry.ResourceEventListener {

    private static final Logger logger = LoggerFactory.getLogger(AclTCRListener.class);

    private final CacheManager cacheManager;

    public AclTCRListener(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public void onUpdate(KylinConfig config, RawResource rawResource) {
        if (Objects.isNull(rawResource)) {
            return;
        }
        getProjectName(rawResource.getResPath()).ifPresent(this::clearCache);
    }

    @Override
    public void onDelete(KylinConfig config, String resPath) {
        getProjectName(resPath).ifPresent(this::clearCache);
    }

    private Optional<String> getProjectName(String resourcePath) {
        if (Objects.isNull(resourcePath)) {
            return Optional.empty();
        }
        String[] elements = resourcePath.split("/");
        // acl resource path like '/{project}/acl/{user|group}/{name}.json
        if (elements.length < 4 || !"acl".equals(elements[2]) || StringUtils.isEmpty(elements[1])) {
            return Optional.empty();
        }
        return Optional.of(elements[1]);
    }

    private void clearCache(String project) {
        logger.debug("clear query cache for {}", project);
        final String suffix = String.format("-%s", project);
        Optional.ofNullable(cacheManager.getCacheNames()).map(Arrays::stream).orElseGet(Stream::empty)
                .filter(name -> name.endsWith(suffix)).forEach(cacheName -> {
                    Ehcache ehcache = cacheManager.getEhcache(cacheName);
                    if (Objects.isNull(ehcache)) {
                        return;
                    }
                    ehcache.removeAll();
                });
    }
}
