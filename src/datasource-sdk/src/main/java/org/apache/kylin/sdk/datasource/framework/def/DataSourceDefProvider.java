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
package org.apache.kylin.sdk.datasource.framework.def;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.sdk.datasource.framework.utils.XmlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class DataSourceDefProvider {
    private static final Logger logger = LoggerFactory.getLogger(DataSourceDefProvider.class);
    private static final String RESOURCE_DIR = "datasource";
    private static final String DATASOURCE_DEFAULT = "default";

    private static final DataSourceDefProvider INSTANCE = new DataSourceDefProvider();
    private final Map<String, DataSourceDef> dsCache = Maps.newConcurrentMap();
    private final ClassLoader cl = getClass().getClassLoader();

    public static DataSourceDefProvider getInstance() {
        return INSTANCE;
    }

    private DataSourceDef loadDataSourceFromEnv(String id) {
        String resourcePath = RESOURCE_DIR + "/" + id + ".xml";
        String resourcePathOverride = resourcePath + ".override";
        InputStream is = null;
        try {
            URL urlOverride, url;
            urlOverride = cl.getResource(resourcePathOverride);
            if (urlOverride == null) {
                url = cl.getResource(resourcePath);
            } else {
                url = urlOverride;
                logger.debug("Use override xml:{}", resourcePathOverride);
            }
            if (url == null)
                return null;

            is = url.openStream();
            DataSourceDef ds = XmlUtil.readValue(is, DataSourceDef.class);
            ds.init();
            return ds;
        } catch (IOException e) {
            logger.error("Failed to load data source: Path={}", resourcePath, e);
            return null;
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    // only for dev purpose
    private DataSourceDef loadDataSourceFromDir(String id) {
        String resourcePath = KylinConfig.getKylinHome() + "/conf/datasource/" + id + ".xml";
        try (InputStream is = new FileInputStream(resourcePath)) {
            DataSourceDef ds = XmlUtil.readValue(is, DataSourceDef.class);
            ds.init();
            return ds;
        } catch (IOException e) {
            logger.error("[Dev Only] Failed to load data source from directory.: Path={}", resourcePath,
                    e.getMessage());
            return null;
        }
    }

    private boolean isDevEnv() {
        return (System.getenv("KYLIN_HOME") != null || System.getProperty("KYLIN_HOME") != null)
                && KylinConfig.getInstanceFromEnv().isDevEnv();
    }

    // ===============================================

    public DataSourceDef getById(String id) {
        if (isDevEnv()) {
            DataSourceDef devDs = loadDataSourceFromDir(id);
            if (devDs != null)
                return devDs;
        }

        DataSourceDef ds = dsCache.get(id);
        if (ds != null)
            return ds;

        synchronized (this) {
            ds = dsCache.get(id);
            if (ds == null) {
                ds = loadDataSourceFromEnv(id);
                if (ds != null)
                    dsCache.put(id, ds);
            }
        }
        return ds;
    }

    public DataSourceDef getDefault() {
        return getById(DATASOURCE_DEFAULT);
    }

}
