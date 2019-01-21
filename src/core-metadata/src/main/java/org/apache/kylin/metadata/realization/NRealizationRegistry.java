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

package org.apache.kylin.metadata.realization;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;

/**
 */
public class NRealizationRegistry {

    private static final Logger logger = LoggerFactory.getLogger(NRealizationRegistry.class);

    public static NRealizationRegistry getInstance(KylinConfig config, String project) {
        return config.getManager(project, NRealizationRegistry.class);
    }

    // called by reflection
    static NRealizationRegistry newInstance(KylinConfig config, String project) throws IOException {
        return new NRealizationRegistry(config, project);
    }

    // ============================================================================

    private Map<String, IRealizationProvider> providers;
    private KylinConfig config;
    private String project;

    public NRealizationRegistry(KylinConfig config, String project) throws IOException {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NRealizationRegistry with KylinConfig Id: {} for project {}",
                    System.identityHashCode(config), project);
        this.config = config;
        this.project = project;
        init();
    }

    private void init() {
        providers = Maps.newConcurrentMap();

        // use reflection to load providers
        String[] providerNames = config.getRealizationProviders();
        for (String clsName : providerNames) {
            try {
                Class<? extends IRealizationProvider> cls = ClassUtil.forName(clsName, IRealizationProvider.class);
                IRealizationProvider p = (IRealizationProvider) cls
                        .getMethod("getInstance", KylinConfig.class, String.class).invoke(null, config, project);
                providers.put(p.getRealizationType(), p);

            } catch (Exception | NoClassDefFoundError e) {
                if (e instanceof ClassNotFoundException || e instanceof NoClassDefFoundError)
                    logger.warn("Failed to create realization provider " + e);
                else
                    logger.error("Failed to create realization provider", e);
            }
        }

        if (providers.isEmpty())
            throw new IllegalArgumentException("Failed to find realization provider");

        logger.info("RealizationRegistry is " + providers);
    }

    public Set<String> getRealizationTypes() {
        return Collections.unmodifiableSet(providers.keySet());
    }

    public IRealization getRealization(String realizationType, String name) {
        IRealizationProvider p = providers.get(realizationType);
        if (p == null) {
            logger.warn("No provider for realization type " + realizationType);
            return null;
        }

        try {
            return p.getRealization(name);
        } catch (Exception ex) {
            // exception is possible if e.g. cube metadata is wrong
            logger.warn("Failed to load realization " + realizationType + ":" + name, ex);
            return null;
        }
    }

}
