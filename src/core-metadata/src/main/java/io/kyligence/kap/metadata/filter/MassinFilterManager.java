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

package io.kyligence.kap.metadata.filter;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.metadata.filter.function.Functions;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Sets;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;

public class MassinFilterManager {
    public static final Logger logger = LoggerFactory.getLogger(MassinFilterManager.class);

    private static final ConcurrentMap<KylinConfig, ResourceStore> RESOURCE_STORE_CACHE = new ConcurrentHashMap<>();
    private final static Cache<String, Pair<Long, Set<ByteArray>>> HDFS_CACHES = CacheBuilder.newBuilder()
            .maximumSize(3).removalListener(new RemovalListener<Object, Object>() {
                @Override
                public void onRemoval(RemovalNotification<Object, Object> notification) {
                    logger.debug(String.valueOf(notification.getCause()));
                }
            }).build();
    private static final ConcurrentMap<String, DimensionEncoding> EncodingMapping = new ConcurrentHashMap<>();

    public static MassinFilterManager getInstance(KylinConfig config) {
        return config.getManager(MassinFilterManager.class);
    }

    // called by reflection
    static MassinFilterManager newInstance(KylinConfig config) throws IOException {
        return new MassinFilterManager(config);
    }

    // ============================================================================

    private KylinConfig kylinConfig;

    private MassinFilterManager(KylinConfig config) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing MassinFilterManager with KylinConfig Id: {}", System.identityHashCode(config));
        this.kylinConfig = config;
    }

    public void setEncoding(String resourceIdentifier, DimensionEncoding encoding) {
        EncodingMapping.put(resourceIdentifier, encoding);
    }

    public static String getResourceIdentifier(KapConfig kapConfig, String filterName) {
        return kapConfig.getMassinResourceIdentiferDir() + "/" + filterName;
    }

    public String save(Functions.FilterTableType filterTableType, List<List<String>> result) throws IOException {
        // Assumption: one column is needed
        String filterName = RandomStringUtils.randomAlphabetic(20);
        String resourcePath = "";

        if (filterTableType == Functions.FilterTableType.HDFS) {
            KapConfig kapConfig = KapConfig.wrap(this.kylinConfig);
            resourcePath = getResourceIdentifier(kapConfig, filterName);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BufferedOutputStream bos = new BufferedOutputStream(baos);
            final Charset cs = Charsets.toCharset("UTF-8");
            for (List<String> line : result) {
                bos.write(line.get(0).toString().getBytes(cs));
                bos.write("\n".getBytes(cs));
            }
            bos.flush();
            bos.close();

            ResourceStore store = getStore();
            store.checkAndPutResource(resourcePath, ByteSource.wrap(baos.toByteArray()), -1);
        } else {
            throw new RuntimeException("HBASE_TABLE FilterTableType Not supported yet");
        }

        ExternalFilterDesc filterDesc = new ExternalFilterDesc();
        filterDesc.setName(filterName);
        filterDesc.setUuid(UUID.randomUUID().toString());
        filterDesc.setFilterResourceIdentifier(resourcePath);
        filterDesc.setFilterTableType(filterTableType);

        //TODO
        throw new IllegalStateException();
        //TableMetadataManager.getInstance(kylinConfig).saveExternalFilter(filterDesc);

        //return filterName;
    }

    public Set<ByteArray> load(Functions.FilterTableType filterTableType, String resourceIdentifier)
            throws IOException {
        if (filterTableType == Functions.FilterTableType.HDFS) {
            Pair<Long, Set<ByteArray>> cached = HDFS_CACHES.getIfPresent(resourceIdentifier);
            if (cached != null) {
                return cached.getSecond();
            }

            Set<ByteArray> ret = Sets.newHashSet();
            ResourceStore store = getStore();
            RawResource rawResource = store.getResource(resourceIdentifier);
            List<String> lines;
            try (InputStream is = rawResource.getByteSource().openStream()) {
                lines = IOUtils.readLines(is, Charset.defaultCharset());
            }

            DimensionEncoding encoding = EncodingMapping.get(resourceIdentifier);
            for (String line : lines) {
                if (StringUtils.isEmpty(line)) {
                    continue;
                }

                try {
                    if (encoding != null) {
                        ByteArray byteArray = ByteArray.allocate(encoding.getLengthOfEncoding());
                        encoding.encode(line, byteArray.array(), 0);
                        ret.add(byteArray);
                    } else {
                        ret.add(new ByteArray(line.getBytes(Charset.defaultCharset())));
                    }
                } catch (Exception e) {
                    throw e;
                }
            }
            return ret;
        } else {
            throw new RuntimeException("HBASE_TABLE FilterTableType Not supported yet");
        }
    }

    private ResourceStore getStore() {
        //        ResourceStore store = RESOURCE_STORE_CACHE.get(kylinConfig);
        //        if (store == null) {
        //            StorageURL url = StorageURL.valueOf(
        //                    kylinConfig.getMetadataUrlPrefix() + "@hdfs,path=" + kylinConfig.getHdfsWorkingDirectory());
        //            try {
        //                store = new HDFSResourceStore(kylinConfig);
        //                synchronized (MassinFilterManager.class) {
        //                    RESOURCE_STORE_CACHE.put(kylinConfig, store);
        //                }
        //            } catch (Exception e) {
        //                throw new RuntimeException("Failed to create HDFSResourceStore at " + url, e);
        //            }
        //        }
        //        return store;
        throw new NotImplementedException("never use it again");
    }
}